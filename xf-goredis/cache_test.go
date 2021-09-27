package xfgoredis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	xfgoredis "github.com/Onefootball/xfetch-go/xf-goredis"
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
)

const (
	key      = "some_key"
	ttl      = 2 * time.Hour
	delta    = time.Second
	readCmd  = "get"
	writeCmd = "set"
)

var ctx = context.Background()

type XFetchGoRedisSuite struct {
	suite.Suite

	client *redis.Client
	server *miniredis.Miniredis
}

func TestXFetchRedigoSuite(t *testing.T) {
	suite.Run(t, &XFetchGoRedisSuite{})
}

func (s *XFetchGoRedisSuite) SetupSuite() {
	server, err := miniredis.Run()
	s.Require().NoError(err)
	s.server = server
}

func (s *XFetchGoRedisSuite) TearDownSuite() {
	s.server.Close()
}

func (s *XFetchGoRedisSuite) SetupTest() {
	client := redis.NewClient(&redis.Options{
		Addr: s.server.Addr(),
	})
	s.client = client
}

func (s *XFetchGoRedisSuite) TearDownTest() {
	s.server.FlushAll()
	defer s.client.Close()
}

func (s *XFetchGoRedisSuite) TestPut() {
	cache := xfgoredis.Wrap(s.client)

	err := cache.Put(ctx, writeCmd, key, ttl, delta, "value")
	s.Assert().Nil(err)

	getValue, err := s.server.Get(key)
	s.Require().NoError(err)
	s.Assert().Equal("value", getValue)

	d, _ := s.server.Get(key + ":delta")
	s.Assert().Equal("1", d)
	s.Assert().Equal(ttl, s.server.TTL(key))
}

func (s *XFetchGoRedisSuite) TestPutWithMoreThanOneArg() {
	cache := xfgoredis.Wrap(s.client)

	err := cache.Put(ctx, writeCmd, key, ttl, delta, "value1", "value2")
	s.Assert().EqualError(err, "length of args was not 1")
}

func (s *XFetchGoRedisSuite) TestReadSuccess() {
	s.server.Set(key, "value")
	ttl := 2 * time.Hour
	s.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err := s.server.Set(deltaKey, "10")
	s.Require().NoError(err)
	s.server.SetTTL(deltaKey, ttl)

	cache := xfgoredis.Wrap(s.client)

	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)
	s.Assert().Equal(10.0, lastDelta)
	s.Assert().Equal(7200.0, remaining)
	s.Assert().NoError(err)

	parsedVal := fmt.Sprintf("%v", val)
	s.Require().NoError(err)
	s.Assert().Equal("value", parsedVal)
}

func (s *XFetchGoRedisSuite) TestReadSuccessWithStructWhenNothingThere() {
	cache := xfgoredis.Wrap(s.client)

	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)
	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().Equal(err.Error(), "error on executing pipeline: redis: nil")
	s.Assert().Nil(val)
}

func (s *XFetchGoRedisSuite) TestGetWhenGetDeltaFails() {
	s.server.Set(key, "value")
	ttl := 2 * time.Hour
	s.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err := s.server.Set(deltaKey, "a10")
	s.Require().NoError(err)
	s.server.SetTTL(deltaKey, ttl)

	cache := xfgoredis.Wrap(s.client)

	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)
	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().EqualError(err, "finding delta: strconv.ParseFloat: parsing \"a10\": invalid syntax")

	s.Assert().Nil(val)
}
