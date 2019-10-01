# XFetch-Redigo

A library for mitigating cache stampedes with the XFetch algorithm.


## Background

XFetch is a probabilistic early recomputation algorithm for mitigating against cache stampedes.

Any process reading the cache may recompute the cache value before it expires, while others carry on reading from the cache.

This means we don't get a scenario where a value is suddenly expired and every process starts performing an expensive computation.

The probability that a cache value will be recomputed increases the closer we get to the end of the TTL. 


## Visualization

I've created a graph here: https://www.desmos.com/calculator/bkjyyz3zlp . It visualizes the probability of recomputing the value of a single key the closer we get to the TTL. 

`a`: the time it took to compute the last value (called `delta`)

`b`: a constant which one can tweak to have earlier or later re-computation. The higher it is the earlier the computation will be.

`y`: The probability of computation 

`d`: The TTL (in the graph, 10 seconds)

You can tweak these values to visualize how the algorithm behaves in different scenarios.

### Papers

1. http://www.vldb.org/pvldb/vol8/p886-vattani.pdf
2. https://www.slideshare.net/RedisLabs/redisconf17-internet-archive-preventing-cache-stampede-with-redis-and-xfetch

## Usage

```go
type arbitraryData struct {
	Something string `redis:"something"`
}

conn, err := redis.Dial("tcp", x.server.Addr())
if err != nil {
    // woops
    return
}
defer conn.Close()

ttl := 10*time.Second
beta := 1.0
fetcher := xfredigo.NewFetcher(ttl, beta)

var data arbitraryData
err := fetcher.Fetch(ctx, conn, key, xfredigo.Struct(&data), recomputer)
if err != nil {
    // do something
    return
}

fmt.Println(data.Something)
```
