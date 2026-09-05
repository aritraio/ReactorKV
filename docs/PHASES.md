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

---## Phase 3 — Slab allocator ✅ (shipped)

**Goal.** Replace per-entry mallocs with a fixed-size-class allocator so
the hot path never touches the heap and memory stays unfragmented.

**Deliverables.** `slab.h`/`slab.c`: power-of-two size classes, per-class
free lists (next pointer stored in the free chunk itself), `mmap`'d slab
pages (~1 MiB each) with lazy growth, 16-byte alignment, stats, and page
lists so `slab_allocator_destroy` `munmap`s everything. `kv_entry` is
consolidated to a single flexible-payload chunk: `kv_store` owns a
`slab_allocator` and the hash map allocates each entry — header, key
bytes, and value bytes — as one slab chunk, so chain pointers point
directly at slab chunks.

Overwrite semantics: a SET/INCR that still fits the entry's chunk updates
the value in place and keeps the entry object (the Phase 4 LRU hook); one
that outgrows the chunk migrates to a larger chunk, rewiring the bucket
link and carrying `expire_at_ms` across, and returns the old chunk to the
slab.

**Acceptance criteria (all met)**
- `make test` green (hashmap, protocol, and a new `test_slab`); `make
  sanitize` (ASan+UBSan) clean; full concurrent stress (`make stress`)
  byte-exact. Tests assert 16-byte chunk alignment, class selection,
  free-list reuse, lazy multi-page growth, and that stats return to zero.
- `make valgrind` clean: every chunk returned to its class and every page
  `munmap`'d.
- Mixed-size synthetic workloads (64 B … 1 MiB values) show flat RSS
  after warm-up: uniform-size churn is byte-flat across rounds; the 1 MiB
  class plateaus after a single page-granular step. (Valgrind wasn't
  re-run on macOS, where it is unsupported; the ASan/UBSan suite covers
  teardown.)

**Notes / deviations from the plan.**
- The plan says “16 classes, 64 B … 1 MiB”, but that range contains 15
  powers of two; the enumerated size classes are the source of truth and
  `SLAB_CLASS_COUNT` is 15.
- Power-of-two classes are kept per the deliverable (waste ≤ ~50% worst
  case at a boundary), not memcached's ~1.25× growth.
- Entry footprints past the 1 MiB cap are refused with an `-ERR out of
  memory` reply (the parser still accepts bulk strings up to its own
  64 MiB cap, so the allocator cap surfaces as a command error, not a
  crash or silent drop).
- The allocator needs a per-class “smallest class ≥ size” query
  (`slab_class_size`) so the entry layer can record chunk sizes for free
  and decide in-place vs. migrate; the core five-function API is
  unchanged.

**Risks addressed.** Chunk-to-class waste is inherent to power-of-two
classes; teardown walks per-class page lists, never chunks.


---

## Phase 4 — LRU eviction + active expiry workers ✅ (shipped)

**Goal.** Bound memory (`maxmemory` policy) and honor TTLs even for keys
that are never accessed again — the concurrency requirement.

**Deliverables.** `lru.h`/`lru.c`: doubly linked list embedded in
`kv_entry` (`lru_prev`/`lru_next`), head = most recent. Membership is
maintained by the hash map (the single place every entry-lifecycle
transition happens) whenever the store attaches its `lru` root, so the
recency list can never go stale; GET hits touch, SET/DEL/migrations keep
it in sync. `store.h`/`store.c`: `maxmemory` (bytes; 0 = unlimited) with
`allkeys-lru` eviction of LRU tails after each write, plus a Redis-style
bounded expiry sweep (`kv_store_expire_cycle`: rotating cursor over
buckets, then budget enforcement). `expire.h`/`expire.c`: a worker thread
waking every 100 ms (10 Hz), each pass sampling a slice of the keyspace
and purging expired entries. Concurrency: one `pthread_rwlock` on
`kv_store` — read-only commands (`GET`/`MGET`/`INFO`) hold the read lock
for the whole handler (keeping borrowed value pointers valid while the
reply is built), mutators hold the write lock, and the worker holds the
write lock only per bounded pass. Stats (all atomic, lock-free to read):
`hits`, `misses`, `evictions`, key count, used bytes; surfaced via a new
`INFO` command and a 5 s server log line. `main.c` gains `-m/--maxmemory
BYTES` (with `k/m/g` suffixes) and `-e/--expire-ms MS` (0 disables the
worker).

**Acceptance criteria (all met)**
- RSS bounded under `maxmemory`: a 512 KiB-value churn stream against an
  `-m 16m` server peaks ~11.5 MB (one slab chunk of slack) and holds.
  Unit tests verify oldest-key-first eviction, single-entry-over-budget
  refusal, and that in-place overwrites (which allocate nothing) never
  evict.
- TTL keys vanish without any client touching them: `EXPIRE 1` keys are
  purged by the worker within ~1 s (verified over TCP with no GETs), and
  identical 1 MiB churn rounds show 0 kB RSS growth between them — the
  worker reclaims expired chunks and the slab reuses them.
- `make tsan` (ThreadSanitizer) passes on the concurrency suite, which
  hammers the store from a reactor-style thread (write-lock SETs,
  read-lock GETs) while the worker runs — no data races. `make test`
  green (hashmap 30,042 + protocol 142 + slab 60,347 + lru 66 + store 106
  + expire 731 checks); `make sanitize` (ASan+UBSan) clean; `make smoke`
  and `make stress` (1,000 conns / 40k requests) byte-exact with clean
  shutdown.

**Notes / deviations from the plan.**
- Expiry sweep uses a rotating bucket *cursor* (deterministic full
  coverage) rather than random sampling per se; the per-pass budget is
  capped (4096) so huge tables can't monopolize CPU, and scales with key
  count so small tables are fully swept every interval.
- The worker's sweep budget is derived from the lock-free key count
  (`keys/10`, min. configured sample) so a full keyspace sweep takes ~1 s
  at the default cadence.
- Reads of expired keys return a miss but never mutate (no purge on
  GET): expired-but-present entries are left strictly to the worker.
  `INFO` key counts can therefore briefly include an expired entry
  awaiting its sweep.
- Eviction counts only `maxmemory` policy evictions; expiry purges are
  tracked separately (never inflate `evictions`).

**Risks addressed.** Lock granularity: GET borrows slab memory into the
reply, so the read lock spans handler execution — with one reactor thread
readers never contend with each other, and recency touches under the read
lock are safe. Worker shutdown uses an `atomic_bool` + `pthread_join`
before the store is destroyed. Wall-clock expiry (`CLOCK_REALTIME`) is
kept per Redis EXPIRE semantics (documented; clock jumps can only advance
or delay purge).

---

## Phase 5 — WAL persistence + benchmarking ✅ (shipped)

**Goal.** Survive crashes and prove the engine with real tooling.

**Deliverables.** `wal.h`/`wal.c`: an append-only log of the *effective*
mutations in RESP form (`SET`/`INCR` verbatim, `EXPIRE` translated to
`PEXPIREAT <epoch-ms>` so restarts never re-base TTLs, and store-driven
removals — DEL, expiry-worker purges, maxmemory evictions — logged as
`DEL` records so replay cannot resurrect keys). Fsync policies (`always`
fsyncs before the reply; `everysec` default uses a background flusher
thread; `no` leaves flushing to the OS; a clean close always fsyncs).
Startup crash recovery re-plays the log through the *existing* command
pipeline (`kv_dispatch`) with store expiry checks frozen (the Redis AOF
loading model) — deletion records make the replay deterministic. A record
torn by a crash is truncated to the last complete record; mid-file
corruption fails closed. `--wal <path>` / `--fsync <policy>` CLI flags;
`PEXPIREAT` added as a client command (it is the persisted form of
EXPIRE). Benchmark harness `tests/bench.py` (`make bench`) + `make
recovery` (kill -9 wire test); results in `docs/BENCHMARKS.md`.

**Acceptance criteria (all met)**
- `kill -9` mid-load, restart → every acknowledged write present, no
  corruption (`make recovery`: 200 SETs + 5 INCRs + DELs + TTLs survive
  across four boot cycles, including a torn-tail injection); unit-level
  recovery in `tests/test_wal.c` (129 checks: record bytes, absolute-TTL
  translation, torn-tail truncation + continued appends, purge/eviction
  DEL ordering, all fsync policies, fail-closed corruption).
- `make sanitize` (ASan+UBSan) clean and `make tsan` clean — the WAL
  append mutex is exercised against the everysec flusher thread under
  ThreadSanitizer. (`make valgrind` is unavailable on modern macOS, as in
  Phases 3–4; the sanitizer suites cover teardown.)
- Benchmark numbers recorded with QPS + latency p50/p99 in
  `docs/BENCHMARKS.md`, including the WAL cost matrix (`off` ~740k SET/s
  → `no`/`everysec` ~412k → `always` ~51k; single-op median 15 µs →
  30 µs). A before/after for the Phase 3 slab and Phase 4 LRU is not
  re-measurable (no retained baseline binaries); the doc records what was
  measured at the time and where (PHASES Phase 3/4 acceptance) plus the
  Phase 5 deltas this harness can measure directly.

**Notes / deviations from the plan.**
- WAL records are the *effective* mutations, not a verbatim command echo:
  EXPIRE becomes PEXPIREAT (absolute), and every store-driven removal
  (expiry purge, eviction, DEL) is logged as DEL. This — plus freezing
  expiry checks during load — makes replay byte-deterministic without a
  snapshot.
- Two threads can append (reactor command records + expiry-worker purge
  DELs), so `wal_append` is serialized by an internal mutex; the everysec
  flusher only fsyncs.
- `no` fsync policy still fsyncs on clean shutdown, so restart after
  SIGTERM is always consistent.
- Benchmark tooling is a bundled Python harness (`tests/bench.py`)
  because memtier/redis-benchmark are not installed on the dev machine;
  it spawns a fresh server per run for like-for-like policy comparison.
  Absolute numbers are a floor (GIL-bound client); the relative WAL costs
  are the point.
- WAL rotation/compaction remains a stretch goal (log growth is
  unbounded).

**Risks addressed.** fsync throughput is bought deliberately with
`always` (~50k write ops/s, documented); `everysec` amortizes fsync away.

---

## Suggested commit checkpoints

1. Phase 1 complete — commit and tag `phase-1`.
2. Phase 2 complete — `evloop` + reactor server; blocking loop deleted.
3. Phase 3: slab ships; `make test` unchanged.
4. Phase 4 (done): LRU + worker + rwlock + INFO stats; new lru/store/expire
   tests, `make tsan` target.
5. Phase 5 (done): WAL + `PEXPIREAT` + `make recovery`/`make bench` +
   `docs/BENCHMARKS.md`.