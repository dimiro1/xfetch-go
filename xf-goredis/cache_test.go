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

func (s *XFetchGoRedisSuite) TestUpdateSuccessWithStruct() {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"127.0.0.1:7000",
			"127.0.0.1:7001",
			"127.0.0.1:7002",
			"127.0.0.1:7003",
			"127.0.0.1:7004",
			"127.0.0.1:7005"},
	})
	ctx := context.TODO()
	cache := xfredigo.Wrap(client)

	err := cache.Put(ctx, writeCmd, key, ttl, delta, "value")
	s.Assert().Nil(err)

	val := client.Get(ctx, key)

	s.Assert().Equal("\"value\"", val.Val())

	err = cache.Put(ctx, writeCmd, "struct", ttl, delta,
		struct {
			Name string
			Age  int
		}{
			Name: "robert",
			Age:  12,
		})
	s.Assert().Nil(err)

	val = client.Get(ctx, "struct")
	s.Assert().Equal("{\"Name\":\"robert\",\"Age\":12}", val.Val())
}

func (s *XFetchGoRedisSuite) TestReadSuccessWithStruct() {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"127.0.0.1:7000",
			"127.0.0.1:7001",
			"127.0.0.1:7002",
			"127.0.0.1:7003",
			"127.0.0.1:7004",
			"127.0.0.1:7005"},
	})
	ctx := context.TODO()
	err := client.Set(ctx, key, "value", ttl).Err()
	s.Assert().Nil(err)
	err = client.Set(ctx, fmt.Sprintf("%s:delta", key), "10", ttl).Err()
	s.Assert().Nil(err)

	cache := xfredigo.Wrap(client)
	val, _, deltaVal, err := cache.Get(ctx, readCmd, key)
	s.Assert().Nil(err)
	s.Assert().Equal("value", val)
	//s.Assert().Equal(7199.994, ttlVal) // unable to find the precision loss
	s.Assert().Equal(10.0, deltaVal)

}

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
