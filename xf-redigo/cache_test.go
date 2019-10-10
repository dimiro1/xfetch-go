package xfredigo_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	xfredigo "github.com/motain/xfetch-go/xf-redigo"
	"github.com/stretchr/testify/suite"
)

const (
	key   = "some_key"
	ttl   = 2 * time.Hour
	delta = 1.0
)

type XFetchRedigoSuite struct {
	suite.Suite

	server *miniredis.Miniredis
	conn   redis.Conn
}

type arbitraryData struct {
	Value string `redis:"value" json:"value"`
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

	data := arbitraryData{"hello"}
	err := cache.Update(key, ttl, delta, xfredigo.Struct(&data))
	s.Assert().NoError(err)

	s.Assert().Equal("hello", s.server.HGet(key, "value"))

	d, _ := s.server.Get(key + ":delta")
	s.Assert().Equal("1", d)
	s.Assert().Equal(ttl, s.server.TTL(key))
}

func (s *XFetchRedigoSuite) TestReadSuccessWithStruct() {
	s.server.HSet(key, "value", "hello")
	ttl := 2 * time.Hour
	s.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err := s.server.Set(deltaKey, "10")
	s.Require().NoError(err)
	s.server.SetTTL(deltaKey, ttl)

	cache := xfredigo.Wrap(s.conn)

	var data arbitraryData
	lastDelta, remaining, err := cache.Read(key, xfredigo.Struct(&data))
	s.Assert().Equal(10.0, lastDelta)
	s.Assert().Equal(7200.0, remaining)
	s.Assert().NoError(err)
	s.Assert().Equal(arbitraryData{"hello"}, data)
}

func (s *XFetchRedigoSuite) TestReadSuccessWithStructWhenNothingThere() {
	cache := xfredigo.Wrap(s.conn)

	var data arbitraryData
	lastDelta, remaining, err := cache.Read(key, xfredigo.Struct(&data))
	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().NoError(err)
	s.Assert().Equal(arbitraryData{}, data)
}

func (s *XFetchRedigoSuite) TestUpdateSuccessWithJSON() {
	cache := xfredigo.Wrap(s.conn)

	data := arbitraryData{"hello"}
	err := cache.Update(key, ttl, delta, xfredigo.JSON(&data))
	s.Assert().NoError(err)

	value, err := s.server.Get(key)
	s.Assert().Equal("{\"value\":\"hello\"}", value)

	d, _ := s.server.Get(key + ":delta")
	s.Assert().Equal("1", d)
	s.Assert().Equal(ttl, s.server.TTL(key))
}

func (s *XFetchRedigoSuite) TestReadSuccessWithJSON() {
	err := s.server.Set(key, "{\"value\":\"hello\"}")
	s.Require().NoError(err)
	ttl := 2 * time.Hour
	s.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err = s.server.Set(deltaKey, "10")
	s.Require().NoError(err)
	s.server.SetTTL(deltaKey, ttl)

	cache := xfredigo.Wrap(s.conn)

	var data arbitraryData
	lastDelta, remaining, err := cache.Read(key, xfredigo.JSON(&data))
	s.Assert().NoError(err)
	s.Assert().Equal(10.0, lastDelta)
	s.Assert().Equal(7200.0, remaining)
}

func (s *XFetchRedigoSuite) TestReadSuccessWithJSONWhenNothingThere() {
	cache := xfredigo.Wrap(s.conn)

	var data arbitraryData
	lastDelta, remaining, err := cache.Read(key, xfredigo.JSON(&data))
	s.Assert().NoError(err)
	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().Equal(arbitraryData{}, data)
}

func (s *XFetchRedigoSuite) TestUpdateSuccessWithMsgpack() {
	cache := xfredigo.Wrap(s.conn)

	data := arbitraryData{"hello"}
	err := cache.Update(key, ttl, delta, xfredigo.Msgpack(&data))
	s.Assert().NoError(err)

	value, err := s.server.Get(key)
	s.Assert().Equal("\x81\xa5Value\xa5hello", value)

	d, _ := s.server.Get(key + ":delta")
	s.Assert().Equal("1", d)
	s.Assert().Equal(ttl, s.server.TTL(key))
}

func (s *XFetchRedigoSuite) TestReadSuccessWithMsgpack() {
	err := s.server.Set(key, "\x81\xa5Value\xa5hello")
	s.Require().NoError(err)
	ttl := 2 * time.Hour
	s.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err = s.server.Set(deltaKey, "10")
	s.Require().NoError(err)
	s.server.SetTTL(deltaKey, ttl)

	cache := xfredigo.Wrap(s.conn)

	var data arbitraryData
	lastDelta, remaining, err := cache.Read(key, xfredigo.Msgpack(&data))
	s.Assert().NoError(err)
	s.Assert().Equal(10.0, lastDelta)
	s.Assert().Equal(7200.0, remaining)
}

func (s *XFetchRedigoSuite) TestReadSuccessWithMsgpackWhenNothingThere() {
	cache := xfredigo.Wrap(s.conn)

	var data arbitraryData
	lastDelta, remaining, err := cache.Read(key, xfredigo.Msgpack(&data))
	s.Assert().NoError(err)
	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().Equal(arbitraryData{}, data)
}
