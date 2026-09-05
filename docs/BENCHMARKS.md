# Benchmarks (Phase 5)

Measured on an Apple Silicon (arm64) Mac running macOS 26, `-O2` build of
this tree, single server process on loopback. The harness is
`tests/bench.py`: it spawns a fresh server per run (so WAL configurations
compare like-for-like), preloads the keyspace for read workloads, and
drives it from Python client threads over TCP. All values are 64 B and the
keyspace is 10,000 distinct keys unless noted.

**Caveats.** The client is Python (GIL-bound), so the *absolute* pipelined
numbers below are a floor — a C client (`redis-benchmark`,
`memtier_benchmark`) would push higher. The *relative* costs (WAL append,
fsync policy) are what to read, and they are robust. Runs are short;
expect ±10% run-to-run on a laptop. To reproduce:

```sh
make
# throughput (pipeline = 100 batches)
python3 tests/bench.py --clients 4 --requests 200000 --pipeline 100 --workload set  --wal off
# latency percentiles (pipeline = 1)
python3 tests/bench.py --clients 1 --requests 50000  --pipeline 1   --workload mixed --wal off
# WAL cost on writes
python3 tests/bench.py --clients 4 --requests 200000 --pipeline 100 --workload set --wal everysec
```

## Baseline throughput (no persistence, 4 clients, pipeline 100)

| Workload | Ops/s | Note |
|----------|------:|------|
| `SET`    | ~740k | pure write path (slab alloc, hash insert) |
| `GET`    | ~660k | read path + reply build |
| `MIXED`  | ~675k | alternating SET/GET |
| `MGET`   | ~47k  | 10 keys/op ⇒ ~475k key lookups/s |

## Baseline latency (1 client, pipeline 1, 50k requests, ms)

| Workload | mean | p50  | p99  | p999 | max |
|----------|-----:|-----:|-----:|-----:|----:|
| `SET`    | 0.016| 0.015| 0.038| 0.055| 0.119 |
| `GET`    | 0.016| 0.016| 0.021| 0.042| 0.146 |
| `MIXED`  | 0.016| 0.016| 0.021| 0.038| 0.107 |

Round-trip on loopback is ~16 µs at the median; the p99 is dominated by
scheduler wakeups, not the engine.

## Write-ahead log cost (SET workload)

| WAL config      | Ops/s (pipe 100) | vs. off | p50 (ms) | p99 (ms) |
|-----------------|-----------------:|--------:|---------:|---------:|
| `off` (baseline)| ~740k            | —       | 0.015    | 0.038    |
| `no`            | ~412k            | −44%    | —        | —        |
| `everysec`      | ~413k            | −44%    | —        | —        |
| `always`        | ~51k             | −93%    | 0.030    | 0.049    |

Reads are unaffected: `GET` with `--wal everysec` measured ~660k ops/s —
identical to the no-persistence baseline, because reads append nothing.

Interpretation:

- **`no` ≈ `everysec`** — exactly as designed. Both `write()` every record
  to the page cache; the difference is only whether an explicit `fsync`
  ever happens while serving (the everysec background thread amortizes
  `fsync` to ~once per second, so its cost disappears from the write
  path). The −44% vs. `off` is the cost of the WAL `write()` syscall per
  mutation.
- **`always`** — a synchronous `fsync` per mutation costs ~15–30 µs on
  this SSD/APFS configuration, dropping pipelined write throughput to
  ~50k ops/s and raising the single-op median from 15 µs to 30 µs. That is
  the price of "an acknowledged write survives `kill -9`"; the two cheaper
  policies bound crash loss to the OS page-cache flush window (~1 s for
  `everysec`).

## Memory

- Entries live in mmap'd slab pages; live memory is bounded by
  `maxmemory` + one chunk under `allkeys-lru` (see docs/PHASES.md Phase 4
  acceptance: 11.5 MB RSS under a 16 MB budget during 512 KiB-value
  churn).
- The WAL adds no RSS: records are staged in one reused buffer and
  `write()`n straight to the page cache. WAL *file* growth is unbounded
  today (rotation/compaction is the documented stretch goal).

## Durability verification (not microbenchmarks)

The recovery suite (`make recovery`, and `tests/test_wal.c`) verifies the
crash contract end-to-end:

- `kill -9` after acknowledged writes (`fsync=always`) → every SET/INCR
  and every DEL/negative-TTL-EXPIRE/past-PEXPIREAT is present after
  restart; no corruption.
- EXPIRE keys survive restart with their original absolute deadline (not
  re-based).
- A torn tail record is truncated to the last complete record and the log
  remains appendable.
- Mid-file corruption refuses startup (fail-closed), rather than serving a
  half-replayed dataset.
- Clean shutdown (`SIGTERM`) fsyncs and replays identically on the next
  boot.

## Before/after notes (phase attribution)

Phase 3 (slab allocator) and Phase 4 (LRU + worker) shipped earlier in
this tree without retained baseline binaries, so their exact per-phase QPS
deltas are not re-measurable here. What was recorded at the time is in
[docs/PHASES.md](docs/PHASES.md): Phase 2→3 kept the full stress suite
(1,000 conns / 40k requests) byte-exact and added the flat-RSS acceptance;
Phase 4 added the eviction/expiry behavior with the RSS and TSan results
above. The Phase 5 deltas that this harness *can* measure directly are the
WAL ones in the table — the new component of this phase.

Sanitizer discipline is the same gate as the microbenchmarks: `make
sanitize` (ASan+UBSan) and `make tsan` both pass on the full suite,
including the WAL tests (append mutex + everysec flusher under
ThreadSanitizer).
