package xfredigo

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	xf "github.com/motain/xfetch-go"
	"github.com/pkg/errors"
)

type client struct {
	conn redis.Conn
}

func Wrap(conn redis.Conn) xf.Client {
	return client{conn: conn}
}

func (c client) Update(key string, ttl time.Duration, delta float64, fetchable xf.Fetchable) error {
	serialized, err := fetchable.Serialize()
	if err != nil {
		return errors.Wrap(err, "serializing recomputed value")
	}

	err = c.conn.Send("MULTI")
	if err != nil {
		return errors.Wrap(err, "sending multi command")
	}

	err = c.conn.Send(fetchable.WriteCmd(), redis.Args{key}.AddFlat(serialized)...)
	if err != nil {
		return errors.Wrap(err, "sending write command")
	}

	err = c.conn.Send("EXPIRE", key, ttl.Seconds())
	if err != nil {
		return errors.Wrap(err, "sending expire")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = c.conn.Send("SET", deltaKey, delta, "EX", ttl.Seconds())
	if err != nil {
		return errors.Wrap(err, "setting delta key")
	}

	_, err = c.conn.Do("EXEC")
	if err != nil {
		return errors.Wrap(err, "sending exec")
	}

	return nil
}

func (c client) Read(key string, fetchable xf.Fetchable) (float64, float64, error) {
	err := c.conn.Send("PTTL", key) // PTTL returns the time-to-live in milliseconds
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending pttl")
	}

	deltaKey := fmt.Sprintf("%s:delta", key)
	err = c.conn.Send("GET", deltaKey)
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending get delta key")
	}

	err = c.conn.Send(fetchable.ReadCmd(), key)
	if err != nil {
		return 0, 0, errors.Wrap(err, "sending read command")
	}

	err = c.conn.Flush()
	if err != nil {
		return 0, 0, errors.Wrap(err, "flushing")
	}

	ttl, err := redis.Int64(c.conn.Receive())
	if err != nil {
		return 0, 0, errors.Wrap(err, "finding ttl")
	}

	delta, err := redis.Float64(c.conn.Receive())
	if err != nil {
		if err == redis.ErrNil {
			return 0, 0, nil
		}

		return 0, 0, errors.Wrap(err, "finding delta")
	}

	v, err := c.conn.Receive()
	if err != nil {
		return 0, 0, errors.Wrap(err, "reading")
	}

	err = fetchable.Deserialize(v)
	if err != nil {
		return delta, 0, errors.Wrap(err, "scanning")
	}

	return delta, float64(ttl) / 1000.0, nil
}
