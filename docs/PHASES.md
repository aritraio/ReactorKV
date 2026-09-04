# Development Plan — 5 Phases

The phases are ordered so each one is independently testable and leaves the
tree green. Cross-phase invariant: **the `kv_store` facade and the
incremental `resp_parser` are the seams** — later phases extend the store
internals and swap the network layer without touching commands or protocol.

---

## Phase 1 — Single-threaded socket + basic hash map ✅ (shipped)

**Goal.** A correct, leak-free cache server you can talk to with
`redis-cli`: full command subset, one client at a time.

**Deliverables.** `common.h` (checked allocators, errors, logging),
`hashmap.h`/`hashmap.c` (chained, binary-safe, FNV-1a, 0.75 load-factor
rehash), `protocol.h`/`protocol.c` (incremental RESP parser + reply
writer), `store.h`/`store.c` (command semantics, lazy expiry),
`commands.h`/`commands.c` (dispatch table), `server.h`/`server.c`
(EINTR-safe blocking accept loop), `main.c` (args + signals), unit tests,
smoke test, Makefile with `test` / `sanitize` / `valgrind` targets.

**Acceptance criteria**
- `make test` passes; `make valgrind` reports zero leaks; `make sanitize`
  (ASan+UBSan) passes.
- `./kvstore -p 7379` + `redis-cli -p 7379` round-trips
  SET/GET/DEL/EXPIRE/INCR/MGET.
- Ctrl-C shuts the server down cleanly with no leaks.
- Commands: `SET key val`, `GET key`, `DEL key...`, `EXPIRE key secs`,
  `INCR key`, `MGET key...` (+ `PING` for tooling).

**Known limitations (fixed later).** One client at a time (blocking);
expiry is lazy only; no persistence; no eviction; `redis-cli` commands that
need server-side support beyond the subset (e.g. `CONFIG`) fail with
"unknown command".

**Risks.** None blocking. `MSG_NOSIGNAL` isn't visible under strict POSIX
feature macros on glibc — guarded with `#ifdef`, `SIGPIPE` is ignored
anyway.

---

## Phase 2 — Event-driven reactor (epoll / kqueue) ✅ (shipped)

**Goal.** Serve many concurrent clients from one thread with non-blocking
I/O — the Redis model — proving the multiplexing requirement.

**Deliverables.** `include/kvstore/evloop.h` (loop API + `kv_conn`);
`src/evloop_epoll.c` (Linux: epoll + timerfd) and `src/evloop_kqueue.c`
(BSD/macOS: kqueue + EVFILT_TIMER) behind one interface, selected by the
Makefile via `uname`; `server.c` rewritten as a reactor: non-blocking
listener + conns, level-triggered interest kept in sync by
`sync_interests()` (WRITE registered only while bytes are buffered, so no
EPOLLOUT storm), per-conn read staging buffer feeding `resp_parser`
(unchanged), per-conn growable reply buffer with `woff` for partial
sends, `EAGAIN` handling everywhere, wbuf high/low water marks for read
backpressure, and a connection free-list so memory stays flat under churn.
`main.c` raises `RLIMIT_NOFILE` so thousands of clients fit under the
soft limit.

**Acceptance criteria (all met)**
- `make test` green (now 30,159 checks); `make sanitize` (ASan+UBSan)
  clean, including a full concurrent-load smoke against an
  ASan-instrumented server — zero leaks at shutdown.
- 1,000 concurrent connections, each pipelining SET/GET, complete with
  byte-exact reply verification (`make stress`, 40k requests in ~2s).
- No busy spin: the loop blocks in `kevent`/`epoll_wait`.
- Memory flat under load (conns recycled from the free list; buffers
  capped by backpressure).

**Bug found & fixed during acceptance.** `resp_parser_reset()` freed argv
but kept the parser's input buffer, which the pipeline-drain needs — but a
conn recycled to a *new* client also went through it, leaking the previous
connection's unparsed bytes (e.g. a mid-request disconnect) into the next
one. Under load that corrupted parsing (huge bogus bulk lengths, query-
buffer cap hits, truncated replies). Fix: a distinct `resp_parser_clear()`
(also discards buffered bytes) is called on conn recycle; regression
covered in `tests/test_protocol.c` (`test_parser_clear_on_reuse`).

**Verified on kqueue (macOS).** The epoll backend mirrors it line-for-line
and is selected automatically on Linux.

---

## Phase 3 — Slab allocator

**Goal.** Replace per-entry mallocs with a fixed-size-class allocator so
the hot path never touches the heap and memory stays unfragmented.

**Deliverables.** `slab.h`/`slab.c`: power-of-two size classes, per-class
free lists, `mmap`'d slab pages with lazy growth, 16-byte alignment,
stats; consolidate `kv_entry` to a single flexible-payload chunk
(`char data[]` holding key+value); `kv_store` gains a `slab_allocator` and
routes entry allocation through it; keep the hash map chain pointers
pointing at slab chunks.

**Acceptance criteria**
- Same unit tests pass against the slab-backed store.
- `make valgrind` clean: every chunk returned, every page `munmap`'d.
- A synthetic mixed-size workload (64B … 1 MiB values) shows flat RSS
  after warm-up (no heap fragmentation growth over time).

**Risks.** Chunk-to-size-class mismatch wastes memory (pick classes to cap
waste ≤ ~12.5%, memcached-style); teardown must walk pages, not chunks, to
free.

---

## Phase 4 — LRU eviction + active expiry workers

**Goal.** Bound memory (`maxmemory` policy) and honor TTLs even for keys
that are never accessed again — the concurrency requirement.

**Deliverables.** `lru.h`/`lru.c`: doubly linked list embedded in
`kv_entry` (`lru_prev`/`lru_next`), head = most recent; touch on
GET/SET; evict tail when `maxmemory` high-water is exceeded. `expire.h`/
`expire.c`: worker thread that periodically samples keys (Redis-style
random sampling) and purges expired ones; `pthread_rwlock` on the store
(readers on GET, writers on SET/DEL/evict); config knobs (`maxmemory`,
`maxmemory-policy allkeys-lru`, expiry interval). Stats: hits, misses,
evictions, key count.

**Acceptance criteria**
- With a small `maxmemory` and a working set that exceeds it, RSS stays
  under the cap and the oldest keys are the ones evicted.
- A key with `EXPIRE 1` disappears within ~1s without any client touching
  it.
- Thread-sanitizer (`-fsanitize=thread`) run of the unit tests passes.

**Risks.** Lock contention between the reactor and worker — keep the worker
batch-oriented and hold the write lock briefly; `volatile sig_atomic_t`
stop flag for worker shutdown; clock jumps (use `CLOCK_REALTIME` per Redis,
document).

---

## Phase 5 — WAL persistence + benchmarking

**Goal.** Survive crashes and prove the engine with real tooling.

**Deliverables.** `wal.h`/`wal.c`: append-only log of mutating commands in
RESP form, length-prefixed records; fsync policies (`everysec` default,
`always`, `no`); startup replay — the Phase 1 parser doubles as the
loader; crash-safe truncation of a torn tail record. Benchmark harness:
`memtier_benchmark --protocol redis -p PORT` across SET/GET/MGET
workloads, results recorded in `docs/BENCHMARKS.md`; optional
`perf`/Instruments profile of the hot path (hash lookup, parser, slab
alloc).

**Acceptance criteria**
- `kill -9` mid-load, restart → all fsynced state present, no corruption.
- `make valgrind` clean on a full benchmark run.
- Benchmark numbers recorded with: QPS, latency p50/p99, and a
  before/after for the slab allocator (Phase 3) and LRU (Phase 4).

**Risks.** fsync throughput (mitigate with `everysec` + a background
flusher); benchmark noise (pin CPU, disable turbo, repeat runs); WAL
growth (add compaction/rotation as a stretch goal).

---

## Suggested commit checkpoints

1. Phase 1 complete — commit and tag `phase-1`.
2. Phase 2 complete — `evloop` + reactor server; blocking loop deleted.
3. Phase 3: slab ships; `make test` unchanged.
4. Phase 4: LRU + worker; `make test` unchanged, add expiry/eviction tests.
5. Phase 5: WAL + `docs/BENCHMARKS.md`.