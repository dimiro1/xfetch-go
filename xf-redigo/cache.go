package xfredigo

import (
	"context"
	"fmt"
	"time"

	xf "github.com/Onefootball/xfetch-go"
	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
)

type cache struct {
	conn redis.Conn
}

// Wrap wraps a redigo Conn and allows it to implement the Cache interface
func Wrap(conn redis.Conn) xf.Cache {
	return cache{conn: conn}
}

func (c cache) Get(ctx context.Context, cmd, key string) (interface{}, float64, float64, error) {
	err := c.conn.Send(cmd, key)
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "sending read command")
	}

	err = c.conn.Send("PTTL", key) // PTTL returns the time-to-live in milliseconds
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "sending pttl")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = c.conn.Send("GET", deltaKey)
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "sending get delta key")
	}

	err = c.conn.Flush()
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "flushing")
	}

	v, err := c.conn.Receive()
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "reading")
	}

	ttl, err := redis.Int64(c.conn.Receive())
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "finding ttl")
	}

	delta, err := redis.Float64(c.conn.Receive())
	if err != nil {
		if err == redis.ErrNil {
			return nil, 0, 0, nil
		}

		return nil, 0, 0, errors.Wrap(err, "finding delta")
	}

	return v, float64(ttl) / 1000.0, delta, nil
}

func (c cache) Put(ctx context.Context, cmd, key string, ttl, delta time.Duration, args ...interface{}) error {
	err := c.conn.Send("MULTI")
	if err != nil {
		return errors.Wrap(err, "sending multi command")
	}

	err = c.conn.Send(cmd, redis.Args{key}.AddFlat(args)...)
	if err != nil {
		return errors.Wrap(err, "sending write command")
	}

	err = c.conn.Send("EXPIRE", key, ttl.Seconds())
	if err != nil {
		return errors.Wrap(err, "sending expire")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = c.conn.Send("SET", deltaKey, delta.Seconds(), "EX", ttl.Seconds())
	if err != nil {
		return errors.Wrap(err, "setting delta key")
	}

	_, err = c.conn.Do("EXEC")
	if err != nil {
		return errors.Wrap(err, "sending exec")
	}

	return nil
}
