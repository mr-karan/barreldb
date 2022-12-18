<p align="center">
<img src="./_docs/logo.png" alt="logo" width="15%" />
</p>

# barreldb

_A disk based key-value store based on [Bitcask](https://en.wikipedia.org/wiki/Bitcask)_.

[![Go Reference](https://pkg.go.dev/badge/github.com/mr-karan/barreldb.svg)](https://pkg.go.dev/github.com/mr-karan/barreldb)
[![Go Report Card](https://goreportcard.com/badge/mr-karan/barreldb)](https://goreportcard.com/report/mr-karan/barreldb)
[![GitHub Actions](https://github.com/mr-karan/barreldb/actions/workflows/build.yml/badge.svg)](https://github.com/mr-karan/barreldb/actions/workflows/build.yml)

---

BarrelDB is a Golang implementation of [Bitcask by Riak](https://riak.com/assets/bitcask-intro.pdf) paper and aims to closely follow the spec.

Bitcask is based on a log-structured hash table to store key-value data on disk. It opens a "datafile" (term used for a Bitcask DB file) in an _append-only_ mode and all the writes are sequentially written to this file. Additionally, it also updates an in-memory hash table which maps the key with the offset of the record in the file. This clever yet simple design decision makes it possible to retrieve records from the disk using a _single_ disk seek.

### Benefits of this approach

- Low Latency: Write queries are handled with a single O(1) disk seek. Keys lookup happen in memory using a hash table lookup. This makes it possible to achieve low latency even with a lot of keys/values in the database. Bitcask also relies on the filesystem read-ahead cache for a faster reads.
- High Throughput: Since the file is opened in "append only" mode, it can handle large volumes of write operations with ease. 
- Predictable performance: The DB has a consistent performance even with growing number of records. This can be seen in benchmarks as well.
- Crash friendly: Bitcask commits each record to the disk and also generates a "hints" file which makes it easy to recover in case of a crash.
- Elegant design: Bitcask achieves a lot just by keeping the architecture simple and relying on filesystem primitives for handling complex scenarios (for eg: backup/recovery, cache etc).
- Ability to handle datasets larger than RAM.

### Limitations

- The main limitation is that all the keys must fit in RAM since they're held inside as an in-memory hash table. A potential workaround for this could be to shard the keys in multiple buckets. Incoming records can be hashed into different buckets based on the key. A shard based approach allows each bucket to have limited RAM usage.

## Internals

You can refer to [Writing a disk-based key-value store in Golang](https://mrkaran.dev/posts/barreldb) blog post to read about the internals of Bitcask which also explains how BarrelDB works.

## Usage

### Library


```go
import (
	"github.com/mr-karan/barreldb"
)

barrel, _ := barrel.Init(barrel.WithDir("data/"))

// Set a key.
barrel.Put("hello", []byte("world"))

// Fetch the key.
v, _ := barrel.Get("hello")

// Delete a key.
barrel.Delete("hello")

// Set with expiry.
barrel.PutEx("hello", []byte("world"), time.Second * 3)
```

For a complete example, visit [examples](./examples/main.go).

### Redis Client

`barreldb` implements the API over a simple Redis-compatible server (`barreldb`):

```
127.0.0.1:6379> set hello world
OK
127.0.0.1:6379> get hello
"world"
127.0.0.1:6379> set goodbye world 10s
OK
127.0.0.1:6379> get goodbye
"world"
127.0.0.1:6379> get goodbye
ERR: invalid key: key is already expired
```

## Benchmarks

Using `make bench`:

```
go test -bench=. -benchmem ./...
HELLO
goos: linux
goarch: amd64
pkg: github.com/mr-karan/barreldb
cpu: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz
BenchmarkPut/DisableSync-8                385432              3712 ns/op        1103.48 MB/s          88 B/op          4 allocs/op
BenchmarkPut/AlwaysSync-8                    222           5510563 ns/op           0.74 MB/s         115 B/op          4 allocs/op
BenchmarkGet-8                            840627              1304 ns/op        3142.20 MB/s        4976 B/op          5 allocs/op
PASS
ok      github.com/mr-karan/barreldb 10.751s
```

Using `redis-benchmark`:

```
$ redis-benchmark -p 6379 -t set -n 10000 -r 100000000
Summary:
  throughput summary: 140845.06 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.196     0.016     0.175     0.255     1.031     2.455

$ redis-benchmark -p 6379 -t set -n 200000 -r 100000000
Summary:
  throughput summary: 143678.17 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.184     0.016     0.183     0.223     0.455     2.183

$ redis-benchmark -p 6379 -t get -n 100000 -r 100000000
Summary:
  throughput summary: 170068.03 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.153     0.040     0.143     0.199     0.367     1.447
```

## References

- [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf)
- [Highscalability article on Bitcask](http://highscalability.com/blog/2011/1/10/riaks-bitcask-a-log-structured-hash-table-for-fast-keyvalue.html)
