# kvstore

A high-performance, asynchronous key-value cache and in-memory storage
engine written from scratch in pure C (C11, POSIX). Built for a placement
portfolio: non-blocking event-driven I/O (`epoll`/`kqueue`), a subset of
the Redis RESP protocol, a custom allocator, reader-writer-locked
concurrency, and LRU eviction — all without third-party event libraries.

**Status: Phases 1–5 complete** (see [docs/PHASES.md](docs/PHASES.md) for
the full plan).

## Features (Phase 1)

- **Protocol**: incremental RESP parser (multibulk requests; simple, error,
  integer, bulk, and multibulk replies). Binary-safe keys and values.
- **Commands**: `SET`, `GET`, `DEL`, `EXPIRE`, `INCR`, `MGET` (+ `PING`
  for redis-cli compatibility).
- **Store**: chained hash map (FNV-1a, power-of-two buckets, load-factor
  rehash) with lazy expiration. `SET` clears TTL; `INCR` keeps it;
  non-positive `EXPIRE` deletes — Redis semantics throughout.
- **Robustness**: every syscall checked, `EINTR` retried, `SIGPIPE`
  neutralized, signal-driven clean shutdown, and valgrind/ASan-clean
  teardown.

## Features (Phase 2)

- **Event loop**: a single-threaded reactor over `epoll` (Linux) or
  `kqueue` (BSD/macOS) — selected automatically by the Makefile. No
  third-party event library.
- **Non-blocking everything**: listener and connections, partial-send
  resumes on `EPOLLOUT`/`EVFILT_WRITE`, read backpressure via
  outbound-buffer watermarks, and a connection free-list so memory stays
  flat under churn.
- **Concurrency**: 1,000 simultaneous clients each pipelining
  SET/GET complete byte-exact (`make stress` — ~40k requests in ~2s).
- **Periodic timers**: `timerfd` (Linux) / `EVFILT_TIMER` (macOS) driving
  the stats heartbeat; the same mechanism hosts the Phase 4 expiry worker.

## Features (Phase 3)

- **Slab allocator**: 15 fixed power-of-two size classes (64 B … 1 MiB),
  each with a singly linked free list (the free pointer lives inside the
  free chunk) and lazily `mmap`'d 1 MiB pages — the hot path never touches
  the heap and pages are reused, so RSS stays flat under churn.
- **Single-chunk entries**: `kv_entry` is one flexible-payload slab chunk
  (`data[]` holds key + value, binary-safe, not NUL-terminated), replacing
  the three `malloc`/`strdup` calls Phase 1 made per entry. Chain pointers
  point straight at slab chunks.
- **In-place overwrites**: a value that still fits its entry's chunk is
  updated in place (entry identity preserved — the Phase 4 LRU hook); one
  that outgrows the chunk migrates to a larger chunk, carrying its TTL
  across.
- **1 MiB cap**: entries whose key+value footprint exceeds the largest
  class are refused with `-ERR out of memory` instead of stored.

## Features (Phase 4)

- **maxmemory + allkeys-lru**: a configurable byte budget
  (`-m/--maxmemory`, suffixes `k/m/g` accepted); when live memory exceeds
  it, least-recently-used entries are evicted — including in-place
  overwrites that never allocate, which are exempt. A single entry that
  alone exceeds the budget is refused up front (`-ERR out of memory`).
- **Active expiry worker**: a background thread wakes every 100 ms
  (Redis's 10 Hz cadence) and runs a bounded Redis-style sweep with a
  rotating bucket cursor, so keys given `EXPIRE` are purged even if no
  client ever reads them again. `-e/--expire-ms 0` disables it.
- **Reader/writer locking**: `kv_store` is guarded by a `pthread_rwlock`
  — read-only commands (`GET`/`MGET`/`INFO`) take the read lock for the
  whole handler (which is what keeps borrowed value pointers valid while
  replies are built), mutators take the write lock, and the expiry worker
  holds the write lock only for each bounded pass. One reactor thread +
  one worker, no data races (`make tsan` clean).
- **LRU bookkeeping for free**: each `kv_entry` embeds `lru_prev`/
  `lru_next`, so recency tracking costs no extra allocations — GET hits
  move the entry to the list head, eviction pops the tail, and the
  hash map keeps membership in sync across inserts, in-place overwrites,
  and migrations.
- **Stats**: `INFO` reports `keys`, `hits`, `misses`, `evictions`,
  `used_memory`, and `maxmemory` (atomic counters, lock-free to read);
  the server also logs them every 5 s.

## Features (Phase 5)

- **Write-ahead log**: every mutation that changes the dataset is appended
  to a WAL file as the RESP multibulk command that produced it
  (`SET`/`INCR`/`PEXPIREAT`/`DEL`), so startup recovery just re-plays the
  log through the ordinary command pipeline. Enabled with
  `--wal <path>`.
- **Absolute TTLs**: `EXPIRE key secs` is logged as
  `PEXPIREAT key <epoch-ms>`, so a restart hours later does not re-base
  the key's TTL. (`PEXPIREAT` is also a client-usable command.)
- **Store-driven DELs are logged too**: `DEL`, expiry-worker purges, and
  `maxmemory` evictions all land in the log, so a crash cannot resurrect
  deleted or evicted keys.
- **Fsync policies**: `--fsync always|everysec|no` (default `everysec`,
  background flusher thread). `always` fsyncs before the reply — an
  acknowledged write survives `kill -9`. A clean shutdown always fsyncs.
- **Crash-safe replay**: the log is parsed with the same incremental RESP
  parser the reactor uses; a record torn by a crash is truncated to the
  last complete record and the log stays appendable. Mid-file corruption
  refuses startup rather than serving a half-replayed dataset.
- **Benchmark harness**: `tests/bench.py` (spawns its own server per run)
  and `make bench`; results in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Build

```sh
make            # builds ./kvstore
make test       # unit tests (hashmap, protocol, slab, lru, store, expire, wal)
make smoke      # end-to-end test over real TCP sockets
make stress     # smoke + 1,000 concurrent pipelined connections
make sanitize   # rebuild tests with AddressSanitizer + UBSan, run them
make tsan       # rebuild the concurrency tests with ThreadSanitizer
make recovery   # kill -9 crash / restart durability test (WAL)
make bench      # benchmark harness (see docs/BENCHMARKS.md)
make valgrind   # run tests under valgrind (leak-check=full)
```

Requires a C11 compiler (`clang`/`gcc`), POSIX APIs, and `make`.
`valgrind` is only needed for the valgrind target (not supported on modern
macOS).

## Run

```sh
./kvstore -p 7379          # listen on 127.0.0.1:7379
./kvstore -a 0.0.0.0 -p 7379
./kvstore -m 256m          # cap memory at 256 MiB, evict LRU tails past it
./kvstore -e 0             # disable the active expiry worker
./kvstore --wal /tmp/rdb.wal               # persist mutations (everysec)
./kvstore --wal /tmp/rdb.wal --fsync always  # ack = durable against kill -9
./kvstore --wal /tmp/rdb.wal --fsync no       # OS-managed flush while running
```

Then, in another terminal:

```sh
redis-cli -p 7379 set foo bar
redis-cli -p 7379 get foo      # "bar"
redis-cli -p 7379 incr counter # (integer) 1
redis-cli -p 7379 expire foo 60
redis-cli -p 7379 pexpireat foo 9999999999999  # absolute ms, Redis-compatible
redis-cli -p 7379 mget foo counter
redis-cli -p 7379 info        # keys / hits / misses / evictions / wal_enabled
```

Ctrl-C (or `kill -TERM`) shuts the server down cleanly; with `--wal` it
fsyncs the log on the way out.

## Project layout

```
include/kvstore/   public headers (one per module)
src/               implementations
tests/             unit tests + smoke.py, recovery.py, bench.py
docs/              ARCHITECTURE.md (diagrams, structs, design decisions)
                   PHASES.md (5-phase roadmap)
                   BENCHMARKS.md (measured throughput/latency, WAL cost)
```

Phase 2 serves thousands of concurrent clients from one thread — the Redis
model. The RESP parser was designed incrementally from day one, so it
slotted into the reactor unchanged.

## Roadmap

1. **Phase 1 (done)** — socket + hash map, full command subset, strict
   memory checks.
2. **Phase 2 (done)** — event-driven reactor (`epoll`/`kqueue`),
   non-blocking I/O, 1k-conn stress-verified.
3. **Phase 3 (done)** — slab allocator (fixed-size classes, no heap fragmentation).
4. **Phase 4 (done)** — LRU eviction (`maxmemory`/allkeys-lru), active
   expiry worker, `pthread_rwlock` concurrency, stats instrumentation.
5. **Phase 5 (done)** — WAL persistence (RESP-form records, fsync
   policies, crash-safe replay) + benchmark harness (`tests/bench.py`,
   results in docs/BENCHMARKS.md).

Details, diagrams, and exact struct layouts in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## License

See [LICENSE](LICENSE).