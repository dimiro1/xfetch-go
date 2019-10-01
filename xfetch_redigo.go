package xfredigo

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
)

type (
	Fetcher interface {
		HMFetch(ctx context.Context, conn redis.Conn, key string, scannable interface{}, recompute Recomputer) error
		Fetch(ctx context.Context, conn redis.Conn, readCmd, writeCmd, key string, scannable interface{}, recompute Recomputer) error
	}

	Scannable interface {
		Scan() error
	}

	Recomputer func(ctx context.Context) (interface{}, error)
	Randomizer func() float64

	fetcher struct {
		ttl        time.Duration
		beta       float64 // a constant. the higher it is the more likely an earlier computation
		randomizer func() float64
	}
)

func NewFetcher(ttl time.Duration, beta float64) Fetcher {
	return fetcher{
		ttl:        ttl,
		beta:       beta,
		randomizer: rand.Float64,
	}
}

func NewFetcherWithRandomizer(ttl time.Duration, beta float64, randomizer Randomizer) Fetcher {
	return fetcher{
		ttl:        ttl,
		beta:       beta,
		randomizer: randomizer,
	}
}

func (f fetcher) HMFetch(ctx context.Context, conn redis.Conn, key string, scannable interface{}, recompute Recomputer) error {
	return f.Fetch(ctx, conn, "HGETALL", "HMSET", key, scannable, recompute)
}

func (f fetcher) Fetch(ctx context.Context, conn redis.Conn, readCmd, writeCmd, key string, scannable interface{}, recompute Recomputer) error {
	delta, ttl, err := f.cacheRead(ctx, conn, readCmd, key, scannable)
	if err != nil {
		return errors.Wrap(err, "reading from cache")
	}

	if f.shouldRefresh(delta, ttl) {
		return f.refreshCache(ctx, conn, writeCmd, key, scannable, recompute)
	}

	return nil
}

func (f fetcher) refreshCache(ctx context.Context, conn redis.Conn, cmd, key string, scannable interface{}, recompute Recomputer) error {
	start := time.Now()
	value, err := recompute(ctx)
	if err != nil {
		return errors.Wrap(err, "recomputing value")
	}
	delta := time.Since(start).Seconds()

	err = copier.Copy(scannable, value)
	if err != nil {
		return errors.Wrap(err, "copying recomputed value to scannable")
	}

	err = conn.Send(cmd, redis.Args{key}.AddFlat(value)...)
	if err != nil {
		return errors.Wrap(err, "sending write command")
	}

	err = conn.Send("EXPIRE", key, f.ttl.Seconds())
	if err != nil {
		return errors.Wrap(err, "sending expire")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = conn.Send("SET", deltaKey, delta, "EX", f.ttl.Seconds())
	if err != nil {
		return errors.Wrap(err, "setting delta key")
	}
	err = conn.Flush()
	if err != nil {
		return errors.Wrap(err, "sending flushing conn")
	}

	return nil
}

func (f fetcher) cacheRead(ctx context.Context, conn redis.Conn, cmd, key string, scannable interface{}) (float64, float64, error) {
	err := conn.Send("PTTL", key) // PTTL returns the time-to-live in milliseconds
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending pttl")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = conn.Send("GET", deltaKey)
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending get delta key")
	}

	err = conn.Send(cmd, key)
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending read command")
	}

	err = conn.Flush()
	if err != nil {
		return 0, 0, errors.Wrap(err, "flushing")
	}

	ttl, err := redis.Int64(conn.Receive())
	if err != nil {
		return 0, 0, errors.Wrap(err, "finding ttl")
	}

	delta, err := redis.Float64(conn.Receive())
	if err != nil {
		if err == redis.ErrNil {
			return 0, 0, nil
		}

		return 0, 0, errors.Wrap(err, "finding delta")
	}

	v, err := redis.Values(conn.Receive())
	if err != nil {
		return 0, 0, errors.Wrap(err, "reading")
	}

	if len(v) == 0 {
		return 0, 0, nil
	}

	err = redis.ScanStruct(v, scannable)
	if err != nil {
		return delta, 0, errors.Wrap(err, "scanning")
	}

	return delta, float64(ttl) / 1000.0, nil
}

// See https://www.desmos.com/calculator/bkjyyz3zlp for a visualization
// -> a is the delta
// -> b is the beta
// -> d is the the expiry window
// -> y is rand.Float64()
// -> x is the ttl
//
// N.B. A cache miss would have ttl <= 0, so this will always return true
func (f fetcher) shouldRefresh(delta, ttl float64) bool {
	return -(delta * f.beta * math.Log(f.randomizer())) >= ttl
}
