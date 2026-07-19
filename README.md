# dist-KV

A distributed Redis-compatible key-value server written from scratch in C11. Speaks RESP and is compatible with any standard Redis client.

Full writeup at [naqvi.dev/projects/dist_kv](https://naqvi.dev/projects/dist_kv).

## Supported Commands

**Key/value & counters** — `GET` · `SET` (`NX`/`XX`/`GET`/`EX`/`PX`) · `DEL` · `INCR` · `DECR` · `INCRBY` · `DECRBY`

**Expiry / TTL** — `EXPIRE` · `PEXPIRE` · `PEXPIREAT` · `TTL` · `PTTL` · `PERSIST`

**Sorted sets** — `ZADD` · `ZSCORE` · `ZREM` · `ZRANGE` (`BYSCORE`/`WITHSCORES`) · `ZPOPMIN` · `BZPOPMIN` (blocking)

**Admin / replication** — `PING` · `COMMAND` · `FLUSHDB` · `WAIT` · `REPLCONF` · `PSYNC`

## Build & Run

```bash 
make release      # optimized build (-O3 -march=native -flto) — use this for benchmarks
make              # debug build (default)
make run-server   # build and start on :6379
```

The build is OS-aware: `kqueue` on macOS, `epoll` on Linux — selected automatically. If you use windows, please don't. 

## Tests

```bash
make test         # full suite: C unit tests + Python integration tests, with coverage
make test-unit    # C unit tests only, no coverage gate
make test-compact # AOF compaction integration test
```

`make test` builds two clang-instrumented binaries — the unit suites exercise the storage/parser/engine layers in-process, the Python suite drives the full server over a socket — merges their coverage, and fails if total line coverage drops below 80%.

## Performance

Benchmarked against Redis 7 on macOS (Apple M-series), release build. All runs: 100K requests, 50 parallel clients, `redis-benchmark`.

### Without Pipelining

| Benchmark | Command | dist-KV | Redis | Ratio |
|-----------|---------|--------:|------:|------:|
| Hot key | SET | 162,074 | 130,378 | **+24%** |
| Hot key | GET | 153,846 | 146,627 | **+5%** |
| Random key | SET | 163,934 | 128,205 | **+28%** |
| Random key | GET | 169,491 | 143,472 | **+18%** |
| Random ZADD | ZADD | 166,944 | 80,971 | **+106%** |

### With Pipelining (`-P 16`)

| Benchmark | Command | dist-KV | Redis | Ratio |
|-----------|---------|--------:|------:|------:|
| Hot key | SET | 1,612,903 | 617,283 | **+161%** |
| Hot key | GET | 2,222,222 | 909,090 | **+144%** |
| Random key | SET | 1,428,571 | 689,655 | **+107%** |
| Random key | GET | 2,222,222 | 1,176,470 | **+89%** |
| Random ZADD | ZADD | 724,637 | 497,512 | **+45%** |

### Latency (non-pipelined)

| | p50 | p95 | p99 | max |
|-|----:|----:|----:|----:|
| **SET dist-KV** | 0.159ms | 0.183ms | 0.783ms | 1.751ms |
| **SET Redis** | 0.311ms | 0.439ms | 0.743ms | 3.471ms |
| **GET dist-KV** | 0.159ms | 0.335ms | 0.615ms | 1.871ms |
| **GET Redis** | 0.271ms | 0.399ms | 0.519ms | 3.063ms |
| **ZADD dist-KV** | 0.151ms | 0.223ms | 0.839ms | 2.127ms |
| **ZADD Redis** | 0.383ms | 0.735ms | 3.671ms | 61.503ms |

<sub>Exact commands can be found at the top of each benchmark result file. see [here.](/benchmarks/)</sub>
## Architecture Highlights

- **Single-threaded event loop** — `kqueue`/`epoll` backend; no locking on the data path
- **Pipelined output buffer** — all responses in a recv batch accumulate in a per-client buffer and ship in one `send()` syscall, which is what drives the 1.6M+ pipelined SET numbers
- **FNV-1a open-chaining hashmap** — `hm_find_or_insert` halves work for new-key `SET`; `_h` variants let callers reuse a computed hash across multiple operations on the same key
- **Sorted sets** — dual-indexed: skip list (O(log N) range queries) + per-ZSet hashmap (O(1) score lookups)
- **Double-buffered async AOF** — main thread appends to an in-memory buffer; a background thread flushes to disk with `O_DSYNC`. No blocking I/O on the hot path
- **Fork-based AOF compaction** — child gets a copy-on-write snapshot via `fork()`, serialises the full store to `compacted.aof`, parent polls with `WNOHANG`. Uses `mmap(MAP_ANON|MAP_PRIVATE)` in the child (malloc is unsafe post-fork in a multithreaded process)
- **Leader–replica replication** — replicas connect via `--replicaof`; handshake uses `REPLCONF`/`PSYNC`; write propagation flows through the same output-buffer machinery as normal clients, so replication adds zero extra syscalls on the leader's hot path

## Job-queue broker mode

dist-KV doubles as a single-node at-least-once job-queue broker for a worker fleet — no new data type, no consensus. The queue is a ZSET (`score` = priority/enqueue-time, `member` = job id); workers claim atomically with `ZPOPMIN`/`BZPOPMIN`, a claimed job moves to a `processing:<id>` key with a visibility-timeout TTL, and a client-side reaper (elected via `SET lock:reaper 1 NX EX`) re-enqueues any job whose worker died before acking. Every piece is a server primitive — the visibility/reaper logic lives in the client, off the single-threaded hot path.

```bash
ZADD jobs 1721400000000 email:42        # producer: enqueue, score = priority/time
BZPOPMIN jobs 0                         # worker: block until a job exists, claim atomically
SET processing:email:42 worker-7 EX 30  # mark in-flight with a 30s visibility timeout
DEL processing:email:42                 # ack on success
# reaper (one worker, elected via SET lock:reaper 1 NX EX 10): any job whose
# processing:* key expired had a dead worker → ZADD it back onto the queue
```

All five build phases are shipped (atomic counters + fused claim → expiry subsystem → `SET` options → blocking `BZPOPMIN` → hardening + integration tests). See `notes/TODO_QUEUE.md` for the design and `tests/integration/test_queue.py` for the end-to-end reaper / crash-recovery / replica proof.

## Next
- Lists (`LPUSH`/`RPUSH`/`BRPOP`) for FIFO queues alongside the ZSET priority queue
