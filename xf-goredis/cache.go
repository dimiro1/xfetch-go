package xfgoredis

import (
	"context"
	"fmt"
	"time"

	xf "github.com/Onefootball/xfetch-go"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type cache struct {
	client redis.UniversalClient
}

// Wrap wraps a goredis Client and allows it to implement the Cache interface
func Wrap(client redis.UniversalClient) xf.Cache {
	return cache{client: client}
}

func (c cache) Get(ctx context.Context, cmd, key string) (interface{}, float64, float64, error) {
	pipe := c.client.Pipeline()
	readPipe := pipe.Do(ctx, cmd, key)
	ttlPipe := pipe.Do(ctx, "pttl", key) // PTTL returns the time-to-live in milliseconds
	deltaPipe := pipe.Get(ctx, fmt.Sprintf("%s:delta", key))

	_, err := pipe.Exec(ctx)
	if err != nil {
		if err == redis.Nil {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, errors.Wrap(err, "error on executing pipeline")
	}

	value, err := readPipe.Result()
	if err != nil {
		if err == redis.Nil {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, errors.Wrap(err, "reading")
	}

	ttl, err := ttlPipe.Int64()
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "finding ttl")
	}
	delta, err := deltaPipe.Float64()
	if err != nil {
		if err == redis.Nil {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, errors.Wrap(err, "finding delta")
	}

	return value, float64(ttl) / 1000.0, delta, nil
}

func (c cache) Put(ctx context.Context, cmd, key string, ttl, delta time.Duration, arguments ...interface{}) error {
	if len(arguments) != 1 {
		return errors.New("length of args was not 1")
	}

	pipe := c.client.Pipeline()
	pipe.Do(ctx, cmd, key, arguments[0])
	pipe.Expire(ctx, key, ttl)
	pipe.SetEX(ctx, fmt.Sprintf("%s:delta", key), delta.Seconds(), ttl)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return errors.Wrap(err, "sending exec")
	}

	return nil
}
