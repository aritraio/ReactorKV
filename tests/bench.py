#!/usr/bin/env python3
"""ReactorKV benchmark harness (Phase 5).

Spawns its own server per run (so WAL off/everysec/always/no compare
like-for-like), then drives it with N client threads issuing pipelined
SET / GET / MGET / mixed workloads over TCP, and reports throughput and
latency percentiles.

Methodology notes (see docs/BENCHMARKS.md):
  * `--pipeline 1` times every request individually -> p50/p99/p99.9 from
    the per-request distribution.
  * `--pipeline N>1` measures whole batches (send + drain N replies);
    per-request latency is not meaningful there, so only throughput is
    reported.
  * Requests are spread over a fixed keyspace with deterministic values,
    so GETs hit resident keys and SETs are true writes (WAL append +
    optional fsync). Read workloads preload the keyspace first (untimed).
  * Runs are short by default; use --requests to lengthen. Numbers are
    single-machine, best-effort — see docs/BENCHMARKS.md for what was
    measured and how to reproduce.

Exit code is non-zero if any reply was wrong or the server died.
"""

import argparse
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(os.path.dirname(HERE), "kvstore")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Reader:
    """Buffered blocking reader for RESP replies, pipelining-friendly."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(1 << 16)
            if not chunk:
                raise RuntimeError("closed mid-reply")
            self.buf += chunk
        i = self.buf.index(b"\r\n")
        line, self.buf = self.buf[:i], self.buf[i + 2:]
        return line

    def _exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(1 << 16)
            if not chunk:
                raise RuntimeError("closed mid-payload")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def skip_one(self):
        """Consume one reply, returning (kind, payload_len) for checks."""
        line = self._line()
        t = line[:1]
        if t == b"+":
            return (b"+", 0)
        if t == b"-":
            raise RuntimeError("server error reply: %r" % line[1:])
        if t == b":":
            return (b":", 0)
        if t == b"$":
            n = int(line[1:])
            if n != -1:
                self._exact(n)
                assert self._exact(2) == b"\r\n"
            return (b"$", n)
        if t == b"*":
            n = int(line[1:])
            for _ in range(n):
                self.skip_one()
            return (b"*", 0)
        raise RuntimeError("unexpected reply %r" % line)


def encode(args):
    out = bytearray(b"*%d\r\n" % len(args))
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return bytes(out)


def build_value(key_idx, size):
    seed = ("%08x" % key_idx).encode()
    return (seed * (size // 8 + 1))[:size]


def ping_until_ready(host, port, proc, deadline_s=10):
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"server exited early rc={proc.returncode}")
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.sendall(encode(["PING"]))
            r = Reader(s)
            ok = r.skip_one() == (b"+", 0)
            s.close()
            if ok:
                return
        except OSError:
            pass
        time.sleep(0.03)
    raise RuntimeError("server did not become ready")


class Worker(threading.Thread):
    def __init__(self, port, args, worker_id):
        super().__init__()
        self.port = port
        self.args = args
        self.worker_id = worker_id
        self.lat = []          # per-op latencies (pipeline == 1 only)
        self.errors = 0
        self.done = 0

    def run(self):
        a = self.args
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=30)
        sock.settimeout(120)
        r = Reader(sock)

        base = a.requests // a.clients
        extra = a.requests % a.clients
        count = base + (1 if self.worker_id < extra else 0)
        measure = a.pipeline == 1
        i = 0
        while i < count:
            n = min(a.pipeline, count - i)
            batch = bytearray()
            kinds = []
            for j in range(n):
                op = i + j
                kidx = op % a.keyspace
                if a.workload == "set":
                    batch += encode(["SET", "k%06d" % kidx,
                                     build_value(kidx, a.value_size)])
                    kinds.append((b"+", 0))
                elif a.workload == "get":
                    batch += encode(["GET", "k%06d" % kidx])
                    kinds.append((b"$", a.value_size))
                elif a.workload == "mget":
                    ks = ["k%06d" % ((kidx + k) % a.keyspace)
                          for k in range(10)]
                    batch += encode(["MGET"] + ks)
                    kinds.append((b"*", 0))
                else:  # mixed: even ops write, odd ops read
                    if op % 2 == 0:
                        batch += encode(["SET", "k%06d" % kidx,
                                         build_value(kidx, a.value_size)])
                        kinds.append((b"+", 0))
                    else:
                        batch += encode(["GET", "k%06d" % kidx])
                        kinds.append((b"$", a.value_size))

            t0 = time.perf_counter()
            sock.sendall(bytes(batch))
            for kind, nbytes in kinds:
                try:
                    got = r.skip_one()
                    if got[0] != kind or (kind == b"$" and got != (b"$", nbytes)):
                        self.errors += 1
                except RuntimeError:
                    self.errors += 1
                    sock.close()
                    return
            t1 = time.perf_counter()
            if measure:
                op_ms = (t1 - t0) * 1e3 / n
                self.lat.extend([op_ms] * n)
            i += n
        self.done = count
        sock.close()


def parse_args(argv):
    ap = argparse.ArgumentParser(description="ReactorKV benchmark")
    ap.add_argument("--server", default=SERVER)
    ap.add_argument("--clients", type=int, default=4)
    ap.add_argument("--requests", type=int, default=100000,
                    help="total timed requests across all clients")
    ap.add_argument("--pipeline", type=int, default=50,
                    help="requests per pipelined batch (1 = per-request "
                         "latency mode)")
    ap.add_argument("--keyspace", type=int, default=10000,
                    help="distinct keys; read workloads preload this space")
    ap.add_argument("--value-size", type=int, default=64)
    ap.add_argument("--workload", choices=["set", "get", "mget", "mixed"],
                    default="mixed")
    ap.add_argument("--wal", choices=["off", "everysec", "always", "no"],
                    default="off", help="WAL mode (server is spawned per run)")
    ap.add_argument("--port", type=int, default=0, help="0 = pick a free one")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-spawn", action="store_true",
                    help="attach to an already-running server (--port); "
                         "WAL flags ignored")
    return ap.parse_args(argv)


def main(argv):
    a = parse_args(argv)
    if not os.path.exists(a.server):
        print(f"{a.server} missing — run make first", file=sys.stderr)
        return 1
    port = a.port or free_port()
    proc = None
    wal_path = None
    try:
        if not a.no_spawn:
            cmd = [a.server, "-p", str(port), "-a", a.host, "-e", "0"]
            if a.wal != "off":
                wal_path = os.path.join(tempfile.gettempdir(),
                                        f"kvc_bench_{os.getpid()}.wal")
                if os.path.exists(wal_path):
                    os.unlink(wal_path)
                cmd += ["--wal", wal_path, "--fsync", a.wal]
            logf = open(os.path.join(tempfile.gettempdir(),
                                     "kvstore_bench.log"), "ab")
            proc = subprocess.Popen(cmd, stdout=logf, stderr=logf)
        ping_until_ready(a.host, port, proc)

        # Preload the keyspace for read workloads (untimed).
        if a.workload in ("get", "mget", "mixed"):
            s = socket.create_connection((a.host, port), timeout=10)
            r = Reader(s)
            B = 200
            for start in range(0, a.keyspace, B):
                batch = bytearray()
                for i in range(start, min(a.keyspace, start + B)):
                    batch += encode(["SET", "k%06d" % i,
                                     build_value(i, a.value_size)])
                s.sendall(bytes(batch))
                for _ in range(min(B, a.keyspace - start)):
                    if r.skip_one()[0] != b"+":
                        raise RuntimeError("preload failed")
            s.close()

        t_start = time.perf_counter()
        workers = [Worker(port, a, w) for w in range(a.clients)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        elapsed = time.perf_counter() - t_start

        done = sum(w.done for w in workers)
        errors = sum(w.errors for w in workers)
        qps = done / elapsed if elapsed > 0 else 0.0
        print(f"# workload={a.workload} clients={a.clients} "
              f"pipeline={a.pipeline} requests={done} "
              f"value={a.value_size}B wal={a.wal}")
        print(f"  elapsed={elapsed:.2f}s  qps={qps:,.0f}  errors={errors}")
        if a.pipeline == 1:
            lats = sorted(x for w in workers for x in w.lat)
            n = len(lats)
            if n:
                def pct(p):
                    return lats[min(n - 1, int(n * p))]
                print(f"  latency ms: mean={statistics.fmean(lats):.3f} "
                      f"p50={pct(0.50):.3f} p99={pct(0.99):.3f} "
                      f"p999={pct(0.999):.3f} max={lats[-1]:.3f}")
        if errors:
            print(f"  FAIL: {errors} reply errors", file=sys.stderr)
            return 1
        return 0
    finally:
        if proc is not None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        if wal_path and os.path.exists(wal_path):
            os.unlink(wal_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
