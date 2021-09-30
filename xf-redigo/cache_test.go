package xfredigo_test

import (
	"context"
	"testing"
	"time"

	xfredigo "github.com/Onefootball/xfetch-go/xf-redigo"
	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/suite"
)

const (
	key      = "some_key"
	ttl      = 2 * time.Hour
	delta    = time.Second
	readCmd  = "GET"
	writeCmd = "SET"
)

var ctx = context.Background()

type XFetchRedigoSuite struct {
	suite.Suite

	server *miniredis.Miniredis
	conn   redis.Conn
}

func TestXFetchRedigoSuite(t *testing.T) {
	suite.Run(t, &XFetchRedigoSuite{})
}

func (s *XFetchRedigoSuite) SetupSuite() {
	server, err := miniredis.Run()
	s.Require().NoError(err)
	s.server = server
}

func (s *XFetchRedigoSuite) TearDownSuite() {
	s.server.Close()
}

func (s *XFetchRedigoSuite) SetupTest() {
	conn, err := redis.Dial("tcp", s.server.Addr())
	s.Require().NoError(err)
	s.conn = conn
}

func (s *XFetchRedigoSuite) TearDownTest() {
	s.server.FlushAll()
	defer s.conn.Close()
}

func (s *XFetchRedigoSuite) TestUpdateSuccessWithStruct() {
	cache := xfredigo.Wrap(s.conn)

	err := cache.Put(ctx, writeCmd, key, ttl, delta, "value")
	s.Assert().NoError(err)

	getValue, err := s.server.Get(key)
	s.Require().NoError(err)
	s.Assert().Equal("value", getValue)

	d, _ := s.server.Get(key + ":delta")
	s.Assert().Equal("1", d)
	s.Assert().Equal(ttl, s.server.TTL(key))
}

func (s *XFetchRedigoSuite) TestReadSuccess() {
	s.server.Set(key, "value")
	ttl := 2 * time.Hour
	s.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err := s.server.Set(deltaKey, "10")
	s.Require().NoError(err)
	s.server.SetTTL(deltaKey, ttl)

	cache := xfredigo.Wrap(s.conn)

	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)
	s.Assert().Equal(10.0, lastDelta)
	s.Assert().Equal(7200.0, remaining)
	s.Assert().NoError(err)

	parsedVal, err := redis.String(val, nil)
	s.Require().NoError(err)
	s.Assert().Equal("value", parsedVal)
}

func (s *XFetchRedigoSuite) TestReadSuccessWithStructWhenNothingThere() {
	cache := xfredigo.Wrap(s.conn)

	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)
	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().Nil(err)
	s.Assert().Nil(val)
}

// skipping assertion for ttl as it return -2, if the key does not exist.
func (s *XFetchRedigoSuite) TestReadWithMissingKeyInCache() {
	ttl := 2 * time.Hour
	deltaKey := key + ":delta"
	err := s.server.Set(deltaKey, "10")
	s.Require().NoError(err)
	s.server.SetTTL(deltaKey, ttl)

	cache := xfredigo.Wrap(s.conn)

	val, _, lastDelta, err := cache.Get(ctx, readCmd, key)
	s.Assert().Equal(10.0, lastDelta)
	s.Assert().Nil(err)
	s.Assert().Nil(val)
}
