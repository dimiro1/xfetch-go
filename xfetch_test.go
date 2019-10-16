package xf_test

import (
	"context"
	"errors"
	"testing"
	"time"

	xf "github.com/Onefootball/xfetch-go"

	"github.com/Onefootball/xfetch-go/mocks"
	"github.com/stretchr/testify/suite"
)

const (
	ttl      = 2 * time.Hour
	beta     = 1
	key      = "some_key"
	getCmd   = "GET"
	writeCmd = "SET"
)

var ctx = context.Background()

func TestXFetchSuite(t *testing.T) {
	suite.Run(t, &XFetchSuite{})
}

type XFetchSuite struct {
	suite.Suite

	cache   *mocks.Cache
	fetcher xf.Fetcher
}

func (s *XFetchSuite) SetupTest() {
	s.cache = &mocks.Cache{}
}

func (s *XFetchSuite) TearDownTest() {
	s.cache.AssertExpectations(s.T())
}

func (s *XFetchSuite) TestGetReturnsErrOnCacheGetErr() {
	fetcher := xf.NewFetcherWithRandomizer(beta, fakeRandomizer)

	s.cache.
		On("Get", ctx, getCmd, key).
		Return(nil, 0.0, 0.0, errors.New("bad"))

	retrieval, err := fetcher.Get(ctx, s.cache, getCmd, key)
	s.Assert().EqualError(err, "getting from cache: bad")
	s.Assert().Nil(retrieval)
}

func (s *XFetchSuite) TestGetReturnsRetrievalWithShouldRefreshTrueIfNoValue() {
	fetcher := xf.NewFetcherWithRandomizer(beta, fakeRandomizer)

	s.cache.
		On("Get", ctx, getCmd, key).
		Return(nil, 0.0, 0.0, nil)

	retrieval, err := fetcher.Get(ctx, s.cache, getCmd, key)
	s.Assert().Nil(err)
	s.Assert().Equal(&xf.Retrieval{
		ShouldRefresh: true,
		Value:         nil,
	}, retrieval)
}

func (s *XFetchSuite) TestGetReturnsRetrievalWithShouldRefreshTrueIfShouldRefreshEarly() {
	fetcher := xf.NewFetcherWithRandomizer(beta, fakeRandomizer)

	s.cache.
		On("Get", ctx, getCmd, key).
		Return("some_value", 1.0, 1000.0, nil)

	retrieval, err := fetcher.Get(ctx, s.cache, getCmd, key)
	s.Assert().Nil(err)
	s.Assert().Equal(&xf.Retrieval{
		ShouldRefresh: true,
		Value:         "some_value",
	}, retrieval)
}

func (s *XFetchSuite) TestGetReturnsRetrievalWithShouldRefreshFalseIfShouldNotRefreshEarly() {
	fetcher := xf.NewFetcherWithRandomizer(beta, fakeRandomizer)

	s.cache.
		On("Get", ctx, getCmd, key).
		Return("some_value", 7199.0, 1000.0, nil)

	retrieval, err := fetcher.Get(ctx, s.cache, getCmd, key)
	s.Assert().Nil(err)
	s.Assert().Equal(&xf.Retrieval{
		ShouldRefresh: false,
		Value:         "some_value",
	}, retrieval)
}

func (s *XFetchSuite) TestPutReturnsCachePutErr() {
	fetcher := xf.NewFetcher(beta)
	s.cache.
		On("Put", ctx, writeCmd, key, ttl, time.Second, "some_value").
		Return(errors.New("bad"))

	err := fetcher.Put(ctx, s.cache, writeCmd, key, ttl, time.Second, "some_value")
	s.Assert().EqualError(err, "putting to cache: bad")
}

func (s *XFetchSuite) TestPutSuccess() {
	fetcher := xf.NewFetcher(beta)
	s.cache.
		On("Put", ctx, writeCmd, key, ttl, time.Second, "some_value").
		Return(nil)

	err := fetcher.Put(ctx, s.cache, writeCmd, key, ttl, time.Second, "some_value")
	s.Assert().Nil(err)
}

func fakeRandomizer() float64 {
	return 0.01
}
