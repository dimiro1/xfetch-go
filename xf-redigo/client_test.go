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

func (x *XFetchRedigoSuite) SetupSuite() {
	s, err := miniredis.Run()
	x.Require().NoError(err)
	x.server = s
}

func (x *XFetchRedigoSuite) TearDownSuite() {
	x.server.Close()
}

func (x *XFetchRedigoSuite) SetupTest() {
	conn, err := redis.Dial("tcp", x.server.Addr())
	x.Require().NoError(err)
	x.conn = conn
}

func (x *XFetchRedigoSuite) TearDownTest() {
	x.server.FlushAll()
	defer x.conn.Close()
}

func (x *XFetchRedigoSuite) TestUpdateSuccessWithStruct() {
	client := xfredigo.Wrap(x.conn)

	data := arbitraryData{"hello"}
	err := client.Update(key, ttl, delta, xfredigo.Struct(&data))
	x.Assert().NoError(err)

	x.Assert().Equal("hello", x.server.HGet(key, "value"))

	d, _ := x.server.Get(key + ":delta")
	x.Assert().Equal("1", d)
	x.Assert().Equal(ttl, x.server.TTL(key))
}

func (x *XFetchRedigoSuite) TestReadSuccessWithStruct() {
	x.server.HSet(key, "value", "hello")
	ttl := 2 * time.Hour
	x.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err := x.server.Set(deltaKey, "10")
	x.Require().NoError(err)
	x.server.SetTTL(deltaKey, ttl)

	client := xfredigo.Wrap(x.conn)

	var data arbitraryData
	lastDelta, remaining, err := client.Read(key, xfredigo.Struct(&data))
	x.Assert().Equal(10.0, lastDelta)
	x.Assert().Equal(7200.0, remaining)
	x.Assert().NoError(err)
	x.Assert().Equal(arbitraryData{"hello"}, data)
}

func (x *XFetchRedigoSuite) TestReadSuccessWithStructWhenNothingThere() {
	client := xfredigo.Wrap(x.conn)

	var data arbitraryData
	lastDelta, remaining, err := client.Read(key, xfredigo.Struct(&data))
	x.Assert().Equal(0.0, lastDelta)
	x.Assert().Equal(0.0, remaining)
	x.Assert().NoError(err)
	x.Assert().Equal(arbitraryData{}, data)
}

func (x *XFetchRedigoSuite) TestUpdateSuccessWithJSON() {
	client := xfredigo.Wrap(x.conn)

	data := arbitraryData{"hello"}
	err := client.Update(key, ttl, delta, xfredigo.JSON(&data))
	x.Assert().NoError(err)

	value, err := x.server.Get(key)
	x.Assert().Equal("{\"value\":\"hello\"}", value)

	d, _ := x.server.Get(key + ":delta")
	x.Assert().Equal("1", d)
	x.Assert().Equal(ttl, x.server.TTL(key))
}

func (x *XFetchRedigoSuite) TestReadSuccessWithJSON() {
	err := x.server.Set(key, "{\"value\":\"hello\"}")
	x.Require().NoError(err)
	ttl := 2 * time.Hour
	x.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err = x.server.Set(deltaKey, "10")
	x.Require().NoError(err)
	x.server.SetTTL(deltaKey, ttl)

	client := xfredigo.Wrap(x.conn)

	var data arbitraryData
	lastDelta, remaining, err := client.Read(key, xfredigo.JSON(&data))
	x.Assert().NoError(err)
	x.Assert().Equal(10.0, lastDelta)
	x.Assert().Equal(7200.0, remaining)
}

func (x *XFetchRedigoSuite) TestReadSuccessWithJSONWhenNothingThere() {
	client := xfredigo.Wrap(x.conn)

	var data arbitraryData
	lastDelta, remaining, err := client.Read(key, xfredigo.JSON(&data))
	x.Assert().NoError(err)
	x.Assert().Equal(0.0, lastDelta)
	x.Assert().Equal(0.0, remaining)
	x.Assert().Equal(arbitraryData{}, data)
}

func (x *XFetchRedigoSuite) TestUpdateSuccessWithMsgpack() {
	client := xfredigo.Wrap(x.conn)

	data := arbitraryData{"hello"}
	err := client.Update(key, ttl, delta, xfredigo.Msgpack(&data))
	x.Assert().NoError(err)

	value, err := x.server.Get(key)
	x.Assert().Equal("\x81\xa5Value\xa5hello", value)

	d, _ := x.server.Get(key + ":delta")
	x.Assert().Equal("1", d)
	x.Assert().Equal(ttl, x.server.TTL(key))
}

func (x *XFetchRedigoSuite) TestReadSuccessWithMsgpack() {
	err := x.server.Set(key, "\x81\xa5Value\xa5hello")
	x.Require().NoError(err)
	ttl := 2 * time.Hour
	x.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err = x.server.Set(deltaKey, "10")
	x.Require().NoError(err)
	x.server.SetTTL(deltaKey, ttl)

	client := xfredigo.Wrap(x.conn)

	var data arbitraryData
	lastDelta, remaining, err := client.Read(key, xfredigo.Msgpack(&data))
	x.Assert().NoError(err)
	x.Assert().Equal(10.0, lastDelta)
	x.Assert().Equal(7200.0, remaining)
}

func (x *XFetchRedigoSuite) TestReadSuccessWithMsgpackWhenNothingThere() {
	client := xfredigo.Wrap(x.conn)

	var data arbitraryData
	lastDelta, remaining, err := client.Read(key, xfredigo.Msgpack(&data))
	x.Assert().NoError(err)
	x.Assert().Equal(0.0, lastDelta)
	x.Assert().Equal(0.0, remaining)
	x.Assert().Equal(arbitraryData{}, data)
}
