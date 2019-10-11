package xf_test

import (
	"context"
	"errors"
	"testing"
	"time"

	xf "github.com/Onefootball/xfetch-go"
	"github.com/Onefootball/xfetch-go/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	ttl  = 2 * time.Hour
	beta = 1
	key  = "some_key"
)

var ctx = context.Background()

func TestXFetchSuite(t *testing.T) {
	suite.Run(t, &XFetchSuite{})
}

type XFetchSuite struct {
	suite.Suite

	cache             *mocks.Cache
	fetchedString     string
	fetchable         stringFetchable
	computedFetchable stringFetchable
	previousSince     func(time.Time) time.Duration
}

func (s *XFetchSuite) SetupTest() {
	s.cache = &mocks.Cache{}
	s.fetchable = newStringFetchable(&s.fetchedString)
	s.computedFetchable = newStringFetchable(func() *string { s := "computed"; return &s }())

	s.previousSince = xf.Since
}

func (s *XFetchSuite) TearDownTest() {
	s.cache.AssertExpectations(s.T())
	xf.Since = s.previousSince
	s.fetchedString = ""
}

func (s *XFetchSuite) TestErrReturnedIfFetchableUnderlyingIsNil() {
	fetcher := xf.NewFetcherWithRandomizer(beta, false, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = false
		return s.computedFetchable, ttl, nil
	}

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, nullFetchable{}, recomputer)
	s.Assert().False(retrieved)
	s.Assert().False(recomputeCalled)
	s.Assert().EqualError(err, "fetchable's underlying value must be a non-nil pointer")
}

func (s *XFetchSuite) TestErrReturnedIfFetchableUnderlyingIsNotPointer() {
	fetcher := xf.NewFetcherWithRandomizer(beta, false, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = false
		return s.computedFetchable, ttl, nil
	}

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, nonPointerFetchable{}, recomputer)
	s.Assert().False(retrieved)
	s.Assert().False(recomputeCalled)
	s.Assert().EqualError(err, "fetchable's underlying value must be a non-nil pointer")
}

func (s *XFetchSuite) TestFetchReturnsReadErrIfRecomputeOnCacheFailureFalse() {
	fetcher := xf.NewFetcherWithRandomizer(beta, false, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = false
		return s.computedFetchable, ttl, nil
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(0.0, 0.0, errors.New("bad"))

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().False(retrieved)
	s.Assert().False(recomputeCalled)
	s.Assert().EqualError(err, "reading from cache: bad")
}

func (s *XFetchSuite) TestFetchRecomputesIfRecomputeOnCacheFailureTrue() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return s.computedFetchable, ttl, nil
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(0.0, 0.0, errors.New("bad")).
		On("Update", ctx, key, ttl, 1.0, s.fetchable).
		Return(nil)

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().True(retrieved)
	s.Assert().True(recomputeCalled)
	s.Assert().EqualError(err, "reading from cache: bad")
	s.Assert().Equal("computed", s.fetchedString)
}

func (s *XFetchSuite) TestRecomputeErrReturnedIfRecomputeOnCacheFailureTrueAndCacheFailure() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return nil, 0, errors.New("bad")
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(0.0, 0.0, errors.New("bad"))

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().False(retrieved)
	s.Assert().True(recomputeCalled)
	s.Assert().EqualError(err, "refreshing after cache failure: recomputing value: bad")
	s.Assert().Empty(s.fetchedString)
}

func (s *XFetchSuite) TestRecomputeNilErrReturnedIfRecomputeOnCacheFailureTrueAndCacheFailure() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return nil, 0, nil
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(0.0, 0.0, errors.New("bad"))

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().False(retrieved)
	s.Assert().True(recomputeCalled)
	s.Assert().EqualError(err, "refreshing after cache failure: nil returned from recomputation")
	s.Assert().Empty(s.fetchedString)
}

func (s *XFetchSuite) TestRecomputeCopyErrReturnedIfRecomputeOnCacheFailureTrueAndCacheFailure() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return newIntFetchable(func() *int { i := 2; return &i }()), ttl, nil
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(0.0, 0.0, errors.New("bad"))

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().False(retrieved)
	s.Assert().True(recomputeCalled)
	s.Assert().EqualError(err, "refreshing after cache failure: fetchable type *string is not assignable to recomputed type *int")
	s.Assert().Empty(s.fetchedString)
}

func (s *XFetchSuite) TestRecomputeUpdateErrReturnedIfRecomputeOnCacheFailureTrueAndCacheFailure() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return s.computedFetchable, ttl, nil
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(0.0, 0.0, errors.New("bad")).
		On("Update", ctx, key, ttl, 1.0, s.fetchable).
		Return(errors.New("bad"))

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().False(retrieved)
	s.Assert().True(recomputeCalled)
	s.Assert().EqualError(err, "refreshing after cache failure: updating cache: bad")
	s.Assert().Equal("computed", s.fetchedString)
}

func (s *XFetchSuite) TestCacheRead() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return s.computedFetchable, ttl, nil
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(1.0, 7200.0, nil).
		Run(func(args mock.Arguments) {
			f := args.Get(2).(stringFetchable)
			*f.v = "read"
		})

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().True(retrieved)
	s.Assert().False(recomputeCalled)
	s.Assert().Nil(err)
	s.Assert().Equal("read", s.fetchedString)
}

func (s *XFetchSuite) TestCacheMissUpdateErr() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return s.computedFetchable, ttl, nil
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(1.0, 0.0, nil).
		Run(func(args mock.Arguments) {
			f := args.Get(2).(stringFetchable)
			*f.v = "read"
		}).
		On("Update", ctx, key, ttl, 1.0, s.fetchable).
		Return(errors.New("bad"))

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().True(retrieved)
	s.Assert().True(recomputeCalled)
	s.Assert().EqualError(err, "updating cache: bad")
	s.Assert().Equal("computed", s.fetchedString)
}

func (s *XFetchSuite) TestCacheMissUpdateSuccess() {
	fetcher := xf.NewFetcherWithRandomizer(beta, true, fakeRandomizer)

	var recomputeCalled bool
	recomputer := func(_ context.Context) (xf.Fetchable, time.Duration, error) {
		recomputeCalled = true
		return s.computedFetchable, ttl, nil
	}

	xf.Since = func(_ time.Time) time.Duration {
		return time.Second
	}

	s.cache.On("Read", ctx, key, s.fetchable).
		Return(1.0, 0.0, nil).
		Run(func(args mock.Arguments) {
			f := args.Get(2).(stringFetchable)
			*f.v = "read"
		}).
		On("Update", ctx, key, ttl, 1.0, s.fetchable).
		Return(nil)

	retrieved, err := fetcher.Fetch(ctx, s.cache, key, s.fetchable, recomputer)
	s.Assert().True(retrieved)
	s.Assert().True(recomputeCalled)
	s.Assert().Nil(err)
	s.Assert().Equal("computed", s.fetchedString)
}

func fakeRandomizer() float64 {
	return 0.01
}

type stringFetchable struct{ v *string }

func newStringFetchable(s *string) stringFetchable {
	return stringFetchable{v: s}
}

func (s stringFetchable) Serialize() ([]interface{}, error) { return []interface{}{*s.v}, nil }

func (s stringFetchable) Deserialize(reply interface{}) error {
	rs := reply.(string)
	s.v = &rs
	return nil
}

func (s stringFetchable) Unwrap() interface{} { return s.v }
func (stringFetchable) WriteCmd() string      { return "SET" }
func (stringFetchable) ReadCmd() string       { return "GET" }

type intFetchable struct{ v *int }

func newIntFetchable(s *int) intFetchable { return intFetchable{v: s} }

func (s intFetchable) Serialize() ([]interface{}, error) { return []interface{}{*s.v}, nil }

func (s intFetchable) Deserialize(reply interface{}) error {
	rs := reply.(int)
	s.v = &rs
	return nil
}

func (s intFetchable) Unwrap() interface{} { return s.v }
func (intFetchable) WriteCmd() string      { return "SET" }
func (intFetchable) ReadCmd() string       { return "GET" }

type nullFetchable struct{}

func (s nullFetchable) Serialize() ([]interface{}, error)   { return nil, nil }
func (s nullFetchable) Deserialize(reply interface{}) error { return nil }
func (s nullFetchable) Unwrap() interface{}                 { return nil }
func (nullFetchable) WriteCmd() string                      { return "SET" }
func (nullFetchable) ReadCmd() string                       { return "GET" }

type nonPointerFetchable struct{}

func (s nonPointerFetchable) Serialize() ([]interface{}, error)   { return nil, nil }
func (s nonPointerFetchable) Deserialize(reply interface{}) error { return nil }
func (s nonPointerFetchable) Unwrap() interface{}                 { return "hello" }
func (nonPointerFetchable) WriteCmd() string                      { return "SET" }
func (nonPointerFetchable) ReadCmd() string                       { return "GET" }
