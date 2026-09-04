#!/usr/bin/env python3
"""End-to-end smoke test for kvstore.

Boots the real server on a random port and talks raw RESP to it over TCP
sockets, verifying exact reply bytes, incremental delivery, pipelining,
and a clean signal-driven shutdown. With --stress, additionally runs a
concurrent load: N simultaneous connections, each pipelining SET/GET
requests, then verifies every reply arrived intact.

Usage:
    make smoke            # functional checks only
    make stress           # functional checks + 1000 concurrent conns
    python3 tests/smoke.py [--stress] [--conns N]
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import threading
import time

try:
    import resource
except ImportError:  # pragma: no cover - non-POSIX
    resource = None

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "kvstore")

failures = 0


def check(cond, label):
    global failures
    if cond:
        print(f"ok:   {label}")
    else:
        failures += 1
        print(f"FAIL: {label}")


def send_recv(port, wire, want, label):
    """Open a fresh connection, send one request, read exactly len(want)
    bytes, and compare."""
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        s.sendall(wire)
        got = s.recv(len(want))
        check(got == want, f"{label}: want {want!r} got {got!r}")


def raise_nofile():
    """Phase 2 opens up to N concurrent client sockets; a default soft fd
    limit (~256) would starve the test before the server does anything."""
    if resource is None:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except (ValueError, OSError):
        pass  # hard cap may be lower; fall back to what we have


def start_server(port):
    log_path = os.path.join(ROOT, "kvstore_smoke.log")
    srv = subprocess.Popen(
        [BIN, "-p", str(port)],
        stdout=open(log_path, "wb"),
        stderr=subprocess.STDOUT,
    )
    # Wait for the listener.
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return srv
        except OSError:
            time.sleep(0.05)
    return None


def resp_set(key, val):
    key, val = key.encode(), val.encode()
    return (b"*3\r\n$3\r\nSET\r\n" +
            f"${len(key)}\r\n".encode() + key + b"\r\n" +
            f"${len(val)}\r\n".encode() + val + b"\r\n")


def resp_get(key):
    key = key.encode()
    return b"*2\r\n$3\r\nGET\r\n" + f"${len(key)}\r\n".encode() + key + b"\r\n"


def recv_exactly(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError(f"connection closed after {len(buf)}/{n} bytes")
        buf += chunk
    return bytes(buf)


errors_lock = threading.Lock()


def stress_worker(port, tid, n_req, barrier, errors):
    """One connection: n_req pipelined SETs then n_req pipelined GETs on
    keys unique to this thread. Values are fixed-width so every reply is a
    predictable size; the whole reply stream is compared byte-for-byte."""
    try:
        barrier.wait(timeout=30)
        with socket.create_connection(("127.0.0.1", port), timeout=30) as s:
            keys = [f"k{tid:04d}_{i:08d}" for i in range(n_req)]
            vals = [f"v{tid:04d}_{i:08d}" for i in range(n_req)]  # 14 bytes
            wire = b"".join(resp_set(k, v) for k, v in zip(keys, vals))
            wire += b"".join(resp_get(k) for k in keys)

            expected = b"".join(b"+OK\r\n" for _ in range(n_req))
            # GET reply: $14\r\n + 14 bytes + \r\n = 22 bytes
            expected += b"".join(b"$14\r\n" + v.encode() + b"\r\n"
                                 for v in vals)

            s.sendall(wire)
            got = recv_exactly(s, len(expected))
            if got != expected:
                with errors_lock:
                    errors.append(f"thread {tid}: reply mismatch "
                                  f"({len(got)}/{len(expected)} bytes)")
    except Exception as exc:  # noqa: BLE001 - report any failure
        with errors_lock:
            errors.append(f"thread {tid}: {exc!r}")


def run_stress(port, n_conns, n_req):
    barrier = threading.Barrier(n_conns)
    errors = []
    threads = [
        threading.Thread(target=stress_worker, args=(port, t, n_req, barrier, errors))
        for t in range(n_conns)
    ]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    hung = sum(1 for t in threads if t.is_alive())
    dt = time.monotonic() - t0
    if hung:
        check(False, f"stress: {hung} threads hung (server wedged?)")
    else:
        check(not errors, f"stress: {n_conns} conns x {2 * n_req} requests "
                          f"({n_conns * 2 * n_req} total) in {dt:.2f}s — "
                          f"{'clean' if not errors else errors[:3]}")
    return len(errors) == 0 and hung == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stress", action="store_true",
                        help="run the concurrent load phase")
    parser.add_argument("--conns", type=int, default=1000,
                        help="concurrent connections for --stress")
    args = parser.parse_args()

    if not os.path.exists(BIN):
        print("server binary not found; run `make` first", file=sys.stderr)
        return 1

    raise_nofile()

    # Bind a free port by letting the OS pick one, then close and reuse it.
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    srv = start_server(port)
    if srv is None:
        print("server never came up", file=sys.stderr)
        return 1
    try:
        send_recv(port, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
                  b"+OK\r\n", "SET foo bar")
        send_recv(port, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
                  b"$3\r\nbar\r\n", "GET foo")
        send_recv(port, b"*2\r\n$3\r\nGET\r\n$6\r\nnope42\r\n",
                  b"$-1\r\n", "GET missing")
        send_recv(port, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n", "PING")
        send_recv(port, b"*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n", b":1\r\n", "INCR")
        send_recv(port, b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nx\r\n$2\r\n10\r\n",
                  b":0\r\n", "EXPIRE missing")

        # Incremental delivery: one byte per send; parser must assemble it.
        wire = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
        with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
            for i in range(len(wire)):
                s.sendall(wire[i:i + 1])
            got = s.recv(len(b"$3\r\nbar\r\n"))
            check(got == b"$3\r\nbar\r\n", "incremental byte-by-byte GET")

        # Pipelining: two requests in one write.
        pipe = (b"*3\r\n$3\r\nSET\r\n$1\r\np\r\n$1\r\n1\r\n"
                b"*2\r\n$3\r\nGET\r\n$1\r\np\r\n")
        with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
            s.sendall(pipe)
            got = s.recv(len(b"+OK\r\n"))
            check(got == b"+OK\r\n", "pipeline reply 1/2")
            got = s.recv(len(b"$1\r\n1\r\n"))
            check(got == b"$1\r\n1\r\n", "pipeline reply 2/2")

        # Protocol error: server replies and drops the connection.
        with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
            s.sendall(b"*0\r\n")
            got = s.recv(64)
            check(got.startswith(b"-ERR Protocol error"), "protocol error reply")
            check(s.recv(64) == b"", "connection dropped after protocol error")

        # Concurrent load (Phase 2 acceptance).
        if args.stress:
            n_req = max(2, 20000 // args.conns)  # ~20k requests total
            check(run_stress(port, args.conns, n_req), "concurrent stress load")

        # Clean shutdown: SIGTERM must exit 0.
        srv.send_signal(signal.SIGTERM)
        rc = srv.wait(timeout=5)
        check(rc == 0, "clean shutdown exit code 0")
        check(srv.poll() is not None, "server process exited")
    finally:
        if srv.poll() is None:
            srv.kill()
            srv.wait()

    print("SMOKE PASS" if failures == 0 else "SMOKE FAIL")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())