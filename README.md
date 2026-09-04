# kvstore

A high-performance, asynchronous key-value cache and in-memory storage
engine written from scratch in pure C (C11, POSIX). Built for a placement
portfolio: non-blocking event-driven I/O (`epoll`/`kqueue`), a subset of
the Redis RESP protocol, a custom allocator, reader-writer-locked
concurrency, and LRU eviction — all without third-party event libraries.

**Status: Phases 1–3 complete** (see [docs/PHASES.md](docs/PHASES.md) for
the full 5-phase plan).

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

## Build

```sh
make            # builds ./kvstore
make test       # unit tests (hashmap, protocol, dispatch)
make smoke      # end-to-end test over real TCP sockets
make stress     # smoke + 1,000 concurrent pipelined connections
make sanitize   # rebuild tests with AddressSanitizer + UBSan, run them
make valgrind   # run tests under valgrind (leak-check=full)
```

Requires a C11 compiler (`clang`/`gcc`), POSIX APIs, and `make`.
`valgrind` is only needed for the valgrind target (not supported on modern
macOS).

## Run

```sh
./kvstore -p 7379          # listen on 127.0.0.1:7379
./kvstore -a 0.0.0.0 -p 7379
```

Then, in another terminal:

```sh
redis-cli -p 7379 set foo bar
redis-cli -p 7379 get foo      # "bar"
redis-cli -p 7379 incr counter # (integer) 1
redis-cli -p 7379 expire foo 60
redis-cli -p 7379 mget foo counter
```

Ctrl-C (or `kill -TERM`) shuts the server down cleanly.

## Project layout

```
include/kvstore/   public headers (one per module)
src/               implementations
tests/             unit tests + smoke.py (end-to-end socket test)
docs/              ARCHITECTURE.md (diagrams, structs, design decisions)
                   PHASES.md (5-phase roadmap)
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
4. **Phase 4** — LRU eviction + active expiry worker (`pthread_rwlock`).
5. **Phase 5** — WAL persistence + `memtier_benchmark` benchmarking.

Details, diagrams, and exact struct layouts in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## License

See [LICENSE](LICENSE).