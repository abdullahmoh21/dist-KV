# dist-KV

A Redis-compatible key-value store built from scratch in C11 — no external dependencies, no libuv, no hiredis. Raw POSIX sockets, a hand-rolled RESP parser, and a custom async event loop.

Sustains **1.29M GET / 1.11M SET requests per second** on localhost with AOF persistence enabled.

---

## What it does

- Handles thousands of concurrent connections through a single-threaded **epoll/kqueue event loop**
- Speaks the **Redis wire protocol (RESP)** — works with `redis-cli`, `redis-benchmark`, and any Redis client library out of the box
- Persists writes to an **append-only log** and replays it on startup for crash recovery
- Compacts the AOF log in the background via **`fork()`** — the parent keeps serving requests at full throughput while the child rewrites the log
- Implements **sorted sets** using a skip list + hashmap dual-index: O(log N) inserts and rank queries, O(1) score lookups

## Performance

Benchmarked with `redis-benchmark -t get,set -n 1,000,000 -c 100 -P 16` (pipelining enabled, AOF on):

| Operation | Throughput       | p50 latency |
|-----------|-----------------|-------------|
| GET       | **1,290,322 RPS** | 0.583 ms   |
| SET       | **1,114,827 RPS** | 0.487 ms   |

Throughput was **doubled from ~600K to 1.2M+ RPS** through a series of low-level optimizations. Full writeup: [naqvi.dev/blog/beating_redis](https://naqvi.dev/blog/beating_redis)

## Supported Commands

| Category    | Commands |
|-------------|----------|
| Strings     | `GET`, `SET`, `DEL` (variadic) |
| Sorted Sets | `ZADD` (variadic score-member), `ZSCORE`, `ZREM` (variadic) |
| Utility     | `PING` |

## Build & Run

```bash
make
./server_build   # listens on :6379
```

## Architecture

```
server.c       — TCP listener, connection lifecycle
event_loop/    — epoll (Linux) / kqueue (macOS)
parser/        — RESP protocol parser
engine/        — command dispatch table
store/         — open-addressing hashmap, skip list, output buffer
aof/           — append-only log, startup replay, background compaction
replication/   — in progress
```

---

> Currently implementing leader/follower replication with a RESP-based sync protocol.
