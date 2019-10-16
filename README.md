# XFetch Go

[![GoDoc](https://godoc.org/github.com/Onefootball/xfetch-go?status.svg)](https://godoc.org/github.com/Onefootball/xfetch-go)

A library for mitigating against cache stampedes with the XFetch algorithm.


## Background

XFetch is a probabilistic early recomputation algorithm for mitigating against cache stampedes.

Any process reading the cache may recompute the cache value before it expires, while others carry on reading from the cache.

This means we don't get a scenario where a value is suddenly expired and every process starts performing an expensive computation.

The probability that a cache value will be recomputed increases the closer we get to the end of the TTL. 

## Visualization

I've created a graph here: https://www.desmos.com/calculator/bkjyyz3zlp . It visualizes the probability of recomputing the value of a single key the closer we get to the TTL. 

`a`: the time it took to compute the last value (called `delta` in the below papers)

`b`: a constant which one can tweak to have earlier or later re-computation (called beta). The higher it is the earlier the computation will be.

`y`: The value of a random float between 0 and 1.

`d`: The key TTL (in the graph, 10 seconds)

You can tweak these values to visualize how the algorithm behaves in different scenarios.

### Papers

1. http://www.vldb.org/pvldb/vol8/p886-vattani.pdf
2. https://www.slideshare.net/RedisLabs/redisconf17-internet-archive-preventing-cache-stampede-with-redis-and-xfetch

## Usage

### With Redigo

```go
type arbitraryData struct {
	Something string `redis:"something"`
}

conn, err := redis.Dial("tcp", x.server.Addr())
if err != nil {
    // handle error
}
defer conn.Close()

beta := 1.0
fetcher := xf.NewFetcher(beta)
ttl := time.Hour

cache := xfredigo.Wrap(conn)

retrieval, err := fetcher.Get(ctx, cache, "HGETALL", "some_key")
if err != nil {
   // handle err
}

if !retrieval.ShouldRefresh {
    redisValue, err := redis.Values(retrieval.Value)
    if err != nil {
       // handle
    }	
    
    var data arbitraryData
    err = redis.ScanStruct(redisValue, &data)
    if err != nil {
       // handle
    }
    return data
}

start := time.Now()
data := fetchData()
delta := time.Since(start)
err = fetcher.Put(ctx, cache, "HMSET", "some_key", ttl, delta, redis.Args{}.AddFlat(prsn)...)
if err != nil {
   // handle
}
```

### With other caches/libraries

Any caching library that implements the `Cache` and `Fetchable` interfaces can be used with this library.

Feel free to send a pull-request if you would like to use this library
with any other cache or caching library.
