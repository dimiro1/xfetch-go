package xf

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Mockable time.Since function for tests
var Since = func(t time.Time) time.Duration {
	return time.Since(t)
}

type (
	// Cache abstracts away cache operations. You can use any kind of cache
	// or caching library to implement it.
	Cache interface {
		// Update does the following:
		// 	- Serializes the fetchable into something that can be passed to a cache
		//	- Writes the fetchable and sets its ttl to the parameter ttl in seconds
		//  - Writes a delta to the cache
		Update(key string, ttl time.Duration, delta float64, fetchable Fetchable) error

		// Read does the following:
		//	- Reads the fetchable at `key`, as well as its TTL
		//	- Reads the delta key
		Read(key string, fetchable Fetchable) (delta float64, ttl float64, err error)
	}

	// Fetcher reads the value of a given key from a cache and decides whether to recompute it.
	// It returns whether a value has been retrieved (bool) and an error.
	Fetcher interface {
		Fetch(ctx context.Context, cache Cache, key string, fetchable Fetchable, recompute Recomputer) (bool, error)
	}

	// Fetchable is an object that can be fetched from the cache.
	Fetchable interface {
		Serialize() ([]interface{}, error)   // Serialize returns the object serialized as a list of arguments
		Deserialize(reply interface{}) error // Deserialize is a function called when scanning from the cache
		Unwrap() interface{}                 // Unwrap returns the underlying value
		WriteCmd() string                    // WriteCmd is the command the library will use when writing to the cache
		ReadCmd() string                     // ReadCmd is the command the library will use when writing to the cache
	}

	// Recomputer is a function called on the event of a cache miss or an early fetch. The first value should be
	// the object to be cached or assigned to the value wrapped in a Fetchable.
	// It also returns the ttl of the item.
	Recomputer func(ctx context.Context) (Fetchable, time.Duration, error)

	// Randomizer returns a random float between 0 and 1.
	Randomizer func() float64

	fetcher struct {
		beta                    float64 // a constant. the higher it is the more likely an earlier computation
		randomizer              func() float64
		recomputeOnCacheFailure bool
	}
)

// NewFetcher takes the ttl of a cache key and its beta value and returns a Fetcher, using rand.Float64 as a randomizer
func NewFetcher(beta float64, recomputeOnCacheFailure bool) Fetcher {
	return fetcher{
		beta:                    beta,
		randomizer:              rand.Float64,
		recomputeOnCacheFailure: recomputeOnCacheFailure,
	}
}

// NewFetcher takes the ttl of a cache key, its beta value and a Randomizer and returns a Fetcher
func NewFetcherWithRandomizer(beta float64, recomputeOnCacheFailure bool, randomizer Randomizer) Fetcher {
	return fetcher{
		beta:                    beta,
		randomizer:              randomizer,
		recomputeOnCacheFailure: recomputeOnCacheFailure,
	}
}

func (f fetcher) Fetch(ctx context.Context, cache Cache, key string, fetchable Fetchable, recompute Recomputer) (bool, error) {
	fetchableValue := reflect.ValueOf(fetchable.Unwrap())
	if fetchableValue.Kind() != reflect.Ptr || fetchableValue.IsNil() {
		return false, errors.New("fetchable's underlying value must be a non-nil pointer")
	}

	delta, ttl, err := cache.Read(key, fetchable)
	if err != nil {
		var retrieved bool
		if f.recomputeOnCacheFailure {
			_, refreshErr := f.refresh(ctx, cache, key, fetchable, recompute)
			if refreshErr != nil {
				return false, errors.Wrap(refreshErr, "refreshing after cache failure")
			}
			retrieved = true
		}
		return retrieved, errors.Wrap(err, "reading from cache")
	}

	if f.shouldRefresh(delta, ttl) {
		return f.refresh(ctx, cache, key, fetchable, recompute)
	}

	return true, nil
}

func (f fetcher) refresh(ctx context.Context, cache Cache, key string, fetchable Fetchable, recompute Recomputer) (bool, error) {
	start := time.Now()
	recomputed, ttl, err := recompute(ctx)
	if err != nil {
		return false, errors.Wrap(err, "recomputing value")
	}
	delta := Since(start).Seconds()

	if recomputed == nil {
		return false, errors.New("nil returned from recomputation")
	}

	if err = assign(fetchable.Unwrap(), recomputed.Unwrap()); err != nil {
		return false, err
	}

	err = cache.Update(key, ttl, delta, fetchable)
	if err != nil {
		return true, errors.Wrap(err, "updating cache")
	}

	return true, nil
}

// See https://www.desmos.com/calculator/bkjyyz3zlp for a visualization
// -> a is the delta
// -> b is the beta
// -> d is the the expiry window
// -> y is rand.Float64()
// -> x is the ttl
//
// N.B. A cache miss would have ttl <= 0, so this will always return true
func (f fetcher) shouldRefresh(delta, ttl float64) bool {
	return -(delta * f.beta * math.Log(f.randomizer())) >= ttl
}
