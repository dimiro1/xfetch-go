package xfredigo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	xfredigo "github.com/motain/xfetch-redigo"
	"github.com/stretchr/testify/suite"
)

var ctx = context.Background()

type XFetchRedigoSuite struct {
	suite.Suite

	server *miniredis.Miniredis
	conn   redis.Conn
}

type arbitraryData struct {
	Value string `redis:"value"`
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
	defer x.conn.Close()
}

func (x *XFetchRedigoSuite) TestNoKeyExistsRecomputeCalled() {
	var recomputeCalled bool
	recomputer := func(_ context.Context) (interface{}, error) {
		recomputeCalled = true
		return arbitraryData{"hello"}, nil
	}

	fetcher := xfredigo.NewFetcherWithRandomizer(10*time.Second, 1, true, func() float64 {
		return 0.001
	})

	var data arbitraryData
	key := "TestNoKeyExistsRecomputeCalled"
	retrieved, err := fetcher.Fetch(ctx, x.conn, key, xfredigo.Struct(&data), recomputer)
	x.True(retrieved)
	x.NoError(err)
	x.True(recomputeCalled)
	x.Equal(arbitraryData{"hello"}, data)
}

func (x *XFetchRedigoSuite) TestNoKeyExistsRecomputesIfRecomputeSetToTrue() {
	x.conn.Close()
	var recomputeCalled bool
	recomputer := func(_ context.Context) (interface{}, error) {
		recomputeCalled = true
		return arbitraryData{"hello"}, nil
	}

	fetcher := xfredigo.NewFetcherWithRandomizer(10*time.Second, 1, true, func() float64 {
		return 0.001
	})

	var data arbitraryData
	key := "TestNoKeyExistsRecomputeCalled"
	retrieved, err := fetcher.Fetch(ctx, x.conn, key, xfredigo.Struct(&data), recomputer)
	x.True(retrieved)
	x.Assert().Error(err)
	x.Assert().Contains(err.Error(), "network connection")
	x.True(recomputeCalled)
}

func (x *XFetchRedigoSuite) TestNoKeyExistsReturnsErrOnRedisFailureIfRecomputeSetToFalse() {
	x.conn.Close()
	var recomputeCalled bool
	recomputer := func(_ context.Context) (interface{}, error) {
		recomputeCalled = true
		return arbitraryData{"hello"}, nil
	}

	fetcher := xfredigo.NewFetcherWithRandomizer(10*time.Second, 1, false, func() float64 {
		return 0.001
	})

	var data arbitraryData
	key := "TestNoKeyExistsRecomputeCalled"
	retrieved, err := fetcher.Fetch(ctx, x.conn, key, xfredigo.Struct(&data), recomputer)
	x.False(retrieved)
	x.Assert().Error(err)
	x.Assert().Contains(err.Error(), "network connection")
	x.False(recomputeCalled)
}

func (x *XFetchRedigoSuite) TestRefreshingErrorsOnNonPointerType() {
	var recomputeCalled bool
	recomputer := func(_ context.Context) (interface{}, error) {
		recomputeCalled = true
		return arbitraryData{"foo"}, nil
	}

	fetcher := xfredigo.NewFetcherWithRandomizer(10*time.Second, 1, true, func() float64 {
		return 0.001
	})

	var data arbitraryData
	key := "TestNoKeyExistsRecomputeCalledWithError"
	retrieved, err := fetcher.Fetch(ctx, x.conn, key, xfredigo.Struct(data), recomputer)
	x.False(retrieved)
	x.EqualError(err, "copying recomputed value to fetchable: copy to value is unaddressable")
	x.True(recomputeCalled)
}

func (x *XFetchRedigoSuite) TestNoKeyExistsRecomputeCalledWithError() {
	var recomputeCalled bool
	recomputer := func(_ context.Context) (interface{}, error) {
		recomputeCalled = true
		return nil, errors.New("bad")
	}

	fetcher := xfredigo.NewFetcherWithRandomizer(10*time.Second, 1, true, func() float64 {
		return 0.001
	})

	var data arbitraryData
	key := "TestNoKeyExistsRecomputeCalledWithError"
	retrieved, err := fetcher.Fetch(ctx, x.conn, key, xfredigo.Struct(&data), recomputer)
	x.False(retrieved)
	x.EqualError(err, "recomputing value: bad")
	x.True(recomputeCalled)
}

func (x *XFetchRedigoSuite) TestKeyExistsAndXFetchOutOfMagicZoneLeadsToCacheRead() {
	key := "TestKeyExistsAndXFetchOutOfMagicZoneLeadsToCacheRead"
	x.server.HSet(key, "value", "hello")
	ttl := 2 * time.Hour
	x.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err := x.server.Set(deltaKey, "10")
	x.Require().NoError(err)
	x.server.SetTTL(deltaKey, ttl)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (interface{}, error) {
		recomputeCalled = true
		return &arbitraryData{"hello again"}, nil
	}

	fetcher := xfredigo.NewFetcherWithRandomizer(ttl, 1, true, func() float64 {
		return 0.001
	})

	var data arbitraryData
	retrieved, err := fetcher.Fetch(ctx, x.conn, key, xfredigo.Struct(&data), recomputer)
	x.True(retrieved)
	x.NoError(err)
	x.False(recomputeCalled)
	x.Equal(arbitraryData{"hello"}, data)
}

func (x *XFetchRedigoSuite) TestKeyExistsAndXFetchInMagicZoneLeadsToRecomputation() {
	key := "TestKeyExistsAndXFetchInMagicZoneLeadsToRecomputation"
	x.server.HSet(key, "value", "hello")
	ttl := time.Second
	x.server.SetTTL(key, ttl)
	deltaKey := key + ":delta"
	err := x.server.Set(deltaKey, "1000")
	x.Require().NoError(err)
	x.server.SetTTL(deltaKey, ttl)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (interface{}, error) {
		recomputeCalled = true
		return &arbitraryData{"hello again"}, nil
	}

	fetcher := xfredigo.NewFetcherWithRandomizer(ttl, 1, true, func() float64 {
		return 0.9
	})

	var data arbitraryData
	retrieved, err := fetcher.Fetch(ctx, x.conn, key, xfredigo.Struct(&data), recomputer)
	x.True(retrieved)
	x.NoError(err)
	x.True(recomputeCalled)
	x.Equal(arbitraryData{"hello again"}, data)
}
