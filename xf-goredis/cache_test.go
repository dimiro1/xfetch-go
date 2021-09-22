package xfgoredis_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	xfredigo "github.com/Onefootball/xfetch-go/xf-goredis"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
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

	client     *redis.Client
	clientMock redismock.ClientMock
}

func TestXFetchRedigoSuite(t *testing.T) {
	suite.Run(t, &XFetchGoRedisSuite{})
}

func (s *XFetchGoRedisSuite) SetupTest() {
	client, clientMock := redismock.NewClientMock()
	s.client = client
	s.clientMock = clientMock
}

// TODO: need to fix the first Set command
// func (s *XFetchGoRedisSuite) TestUpdateSuccessWithStruct() {
// 	cache := xfredigo.Wrap(s.client)
// 	s.clientMock.ExpectSet(key, "value", ttl)
// 	s.clientMock.ExpectExpire(key, ttl).SetVal(true)
// 	s.clientMock.ExpectMSet(fmt.Sprintf("%s:delta", key), delta.Seconds(), "EX", ttl).SetVal("")
// 	err := cache.Put(ctx, writeCmd, key, ttl, delta, "value")

// 	s.Assert().NoError(err)
// }

func (s *XFetchGoRedisSuite) TestReadSuccess() {

	cache := xfredigo.Wrap(s.client)
	s.clientMock.ExpectGet(key).SetVal("value")
	s.clientMock.ExpectPTTL(key).SetVal(ttl)
	s.clientMock.ExpectGet(fmt.Sprintf("%s:delta", key)).SetVal("10")
	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)

	s.Assert().Equal(10.0, lastDelta)
	s.Assert().Equal(7200.0, remaining)
	s.Assert().NoError(err)
	parsedVal := fmt.Sprintf("%v", val)
	s.Assert().Equal("value", parsedVal)
}

func (s *XFetchGoRedisSuite) TestGetWhenGetKeyFails() {

	cache := xfredigo.Wrap(s.client)
	s.clientMock.ExpectGet(key).SetErr(errors.New("error occured"))
	s.clientMock.ExpectPTTL(key).SetVal(ttl)
	s.clientMock.ExpectGet(fmt.Sprintf("%s:delta", key)).SetVal("10")
	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)

	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().EqualError(err, "error on executing pipeline: error occured")
	s.Assert().Nil(val)
}

func (s *XFetchGoRedisSuite) TestGetWhenGetDeltaFails() {

	cache := xfredigo.Wrap(s.client)
	s.clientMock.ExpectGet(key).SetVal("value")
	s.clientMock.ExpectPTTL(key).SetVal(ttl)
	s.clientMock.ExpectGet(fmt.Sprintf("%s:delta", key)).SetVal("1a0")
	val, remaining, lastDelta, err := cache.Get(ctx, readCmd, key)

	s.Assert().Equal(0.0, lastDelta)
	s.Assert().Equal(0.0, remaining)
	s.Assert().EqualError(err, "finding delta: strconv.ParseFloat: parsing \"1a0\": invalid syntax")
	s.Assert().Nil(val)
}
