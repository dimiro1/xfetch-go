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

func init() {
	rand.Seed(time.Now().UnixNano())
}

type (
	// Fetcher reads the value of a given key from a cache and decides whether to recompute it.
	// It returns whether a value has been retrieved (bool) and an error.
	Fetcher interface {
		Fetch(ctx context.Context, conn redis.Conn, key string, fetchable Fetchable, recompute Recomputer) (bool, error)
	}

	// Recomputer is a function called on the event of a cache miss or an early fetch. The first value should be
	// the object to be cached or assigned to the value wrapped in a Fetchable.
	Recomputer func(ctx context.Context) (interface{}, error)

	// Randomizer returns a random float between 0 and 1.
	Randomizer func() float64

	fetcher struct {
		ttl                     time.Duration
		beta                    float64 // a constant. the higher it is the more likely an earlier computation
		randomizer              func() float64
		recomputeOnCacheFailure bool
	}
)

// NewFetcher takes the ttl of a cache key and its beta value and returns a Fetcher, using rand.Float64 as a randomizer
func NewFetcher(ttl time.Duration, beta float64, recomputeOnCacheFailure bool) Fetcher {
	return fetcher{
		ttl:                     ttl,
		beta:                    beta,
		randomizer:              rand.Float64,
		recomputeOnCacheFailure: recomputeOnCacheFailure,
	}
}

// NewFetcher takes the ttl of a cache key, its beta value and a Randomizer and returns a Fetcher
func NewFetcherWithRandomizer(ttl time.Duration, beta float64, recomputeOnCacheFailure bool, randomizer Randomizer) Fetcher {
	return fetcher{
		ttl:                     ttl,
		beta:                    beta,
		randomizer:              randomizer,
		recomputeOnCacheFailure: recomputeOnCacheFailure,
	}
}

func (f fetcher) Fetch(ctx context.Context, conn redis.Conn, key string, fetchable Fetchable, recompute Recomputer) (bool, error) {
	delta, ttl, err := f.cacheRead(ctx, conn, key, fetchable)
	if err != nil {
		if f.recomputeOnCacheFailure {
			return f.refreshCache(ctx, conn, key, fetchable, recompute)
		}
		return false, errors.Wrap(err, "reading from cache")
	}

	if f.shouldRefresh(delta, ttl) {
		return f.refreshCache(ctx, conn, key, fetchable, recompute)
	}

	return true, nil
}

func (f fetcher) refreshCache(ctx context.Context, conn redis.Conn, key string, fetchable Fetchable, recompute Recomputer) (bool, error) {
	start := time.Now()
	value, err := recompute(ctx)
	if err != nil {
		return false, errors.Wrap(err, "recomputing value")
	}
	delta := time.Since(start).Seconds()

	err = copier.Copy(fetchable.Value(), value)
	if err != nil {
		return false, errors.Wrap(err, "copying recomputed value to fetchable")
	}

	err = conn.Send(fetchable.WriteCmd(), redis.Args{key}.AddFlat(value)...)
	if err != nil {
		return true, errors.Wrap(err, "sending write command")
	}

	err = conn.Send("EXPIRE", key, f.ttl.Seconds())
	if err != nil {
		return true, errors.Wrap(err, "sending expire")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = conn.Send("SET", deltaKey, delta, "EX", f.ttl.Seconds())
	if err != nil {
		return true, errors.Wrap(err, "setting delta key")
	}
	err = conn.Flush()
	if err != nil {
		return true, errors.Wrap(err, "sending flushing conn")
	}

	return true, nil
}

func (f fetcher) cacheRead(ctx context.Context, conn redis.Conn, key string, fetchable Fetchable) (float64, float64, error) {
	err := conn.Send("PTTL", key) // PTTL returns the time-to-live in milliseconds
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending pttl")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = conn.Send("GET", deltaKey)
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending get delta key")
	}

	err = conn.Send(fetchable.ReadCmd(), key)
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

	err = fetchable.Scan(v)
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
