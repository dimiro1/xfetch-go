package xf

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type (
	// Cache abstracts away cache operations. You can use any kind of cache
	// or caching library to implement it.
	Cache interface {
		Get(ctx context.Context, cmd, key string) (interface{}, float64, float64, error)
		Put(ctx context.Context, cmd, key string, ttl, delta time.Duration, args ...interface{}) error
	}

	// Fetcher reads the value of a given key from a cache and decides whether to recompute it.
	// It returns whether a value has been retrieved (bool) and an error.
	Fetcher interface {
		Get(ctx context.Context, cache Cache, cmd, key string) (*Retrieval, error)
		Put(ctx context.Context, cache Cache, cmd, key string, ttl, delta time.Duration, args ...interface{}) error
	}

	// Randomizer returns a random float between 0 and 1.
	Randomizer func() float64

	fetcher struct {
		beta       float64 // a constant. the higher it is the more likely an earlier computation
		randomizer func() float64
	}

	Retrieval struct {
		ShouldRefresh bool
		Value         interface{}
	}
)

// NewFetcher takes the ttl of a cache key and its beta value and returns a Fetcher, using rand.Float64 as a randomizer
func NewFetcher(beta float64) Fetcher {
	return fetcher{
		beta:       beta,
		randomizer: rand.Float64,
	}
}

// NewFetcher takes the ttl of a cache key, its beta value and a Randomizer and returns a Fetcher
func NewFetcherWithRandomizer(beta float64, randomizer Randomizer) Fetcher {
	return fetcher{
		beta:       beta,
		randomizer: randomizer,
	}
}

func (f fetcher) Get(ctx context.Context, cache Cache, cmd, key string) (*Retrieval, error) {
	val, ttl, delta, err := cache.Get(ctx, cmd, key)
	if err != nil {
		return nil, errors.Wrap(err, "getting from cache")
	}

	return &Retrieval{
		ShouldRefresh: f.shouldRefresh(ttl, delta),
		Value:         val,
	}, nil
}

func (f fetcher) Put(ctx context.Context, cache Cache, cmd, key string, ttl, delta time.Duration, args ...interface{}) error {
	return errors.Wrap(cache.Put(ctx, cmd, key, ttl, delta, args...), "putting to cache")
}

// See https://www.desmos.com/calculator/bkjyyz3zlp for a visualization
// -> a is the delta
// -> b is the beta
// -> d is the the expiry window
// -> y is rand.Float64()
// -> x is the ttl
//
// N.B. A cache miss would have ttl <= 0, so this will always return true
func (f fetcher) shouldRefresh(ttl, delta float64) bool {
	return -(delta * f.beta * math.Log(f.randomizer())) >= ttl
}
