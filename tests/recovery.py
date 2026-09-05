#!/usr/bin/env python3
"""Phase 5 crash-recovery test (wire level).

Starts the real server with a WAL, writes a deterministic dataset, then
SIGKILLs it (no clean close — the crash-safety test) and restarts,
verifying:

  1. every acknowledged write survives (SET / INCR / PEXPIREAT),
  2. deletions survive (DEL, negative-TTL EXPIRE),
  3. a torn tail record (simulated by appending partial bytes to the WAL
     after the crash) is truncated on restart and the log stays usable,
  4. clean SIGTERM shutdown still replays correctly on the next boot.

fsync=always is used so an acknowledged reply implies durability; that is
what makes the kill -9 assertions meaningful.
"""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(os.path.dirname(HERE), "kvstore")

failures = 0


def fail(msg):
    global failures
    failures += 1
    print(f"FAIL: {msg}")


def ok(msg):
    print(f"  ok: {msg}")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Resp:
    """Minimal blocking RESP client for the test surface used here."""

    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.sock.settimeout(10)
        self.buf = b""

    def close(self):
        self.sock.close()

    def _fill(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("connection closed mid-reply")
            self.buf += chunk

    def _line(self):
        self._fill()
        i = self.buf.index(b"\r\n")
        line, self.buf = self.buf[:i], self.buf[i + 2:]
        return line

    def _exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("connection closed mid-payload")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def read_reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return ("simple", rest)
        if t == b"-":
            return ("error", rest)
        if t == b":":
            return ("int", int(rest))
        if t == b"$":
            n = int(rest)
            if n == -1:
                return ("nil", None)
            data = self._exact(n)
            assert self._exact(2) == b"\r\n"
            return ("bulk", data)
        if t == b"*":
            n = int(rest)
            if n == -1:
                return ("nil", None)
            return ("array", [self.read_reply() for _ in range(n)])
        raise RuntimeError(f"unknown reply type {t!r}")

    def cmd(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            if isinstance(a, str):
                a = a.encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)
        return self.read_reply()


def start_server(port, wal_path):
    logf = open(os.path.join(tempfile.gettempdir(), "kvstore_recovery.log"), "ab")
    proc = subprocess.Popen(
        [SERVER, "-p", str(port), "-a", "127.0.0.1", "-e", "0",
         "--wal", wal_path, "--fsync", "always"],
        stdout=logf, stderr=logf)
    # Wait until PING works (bounded).
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            fail(f"server exited early (rc={proc.returncode})")
            return None
        try:
            c = Resp("127.0.0.1", port)
            if c.cmd("PING") == ("simple", b"PONG"):
                c.close()
                return proc
            c.close()
        except OSError:
            time.sleep(0.05)
    fail("server did not become ready")
    return None


def stop_server(proc, hard):
    if proc is None:
        return
    if hard:
        proc.send_signal(signal.SIGKILL)
    else:
        proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        fail("server did not exit after signal")


def main():
    if not os.path.exists(SERVER):
        print(f"build the server first: {SERVER} missing (run make)")
        return 1

    port = free_port()
    wal_path = os.path.join(tempfile.gettempdir(),
                            f"kvc_recovery_{os.getpid()}.wal")
    for p in (wal_path,):
        if os.path.exists(p):
            os.unlink(p)

    N = 200
    # ---- boot 1: load the dataset ----
    print("== boot 1: load")
    proc = start_server(port, wal_path)
    if proc is None:
        return 1
    c = Resp("127.0.0.1", port)

    for i in range(N):
        r = c.cmd("SET", f"k{i:03d}", f"v{i:03d}")
        assert r == ("simple", b"OK"), r
    for _ in range(5):
        assert c.cmd("INCR", "ctr") == ("int", _ + 1), _
    assert c.cmd("SET", "ttl", "x") == ("simple", b"OK")
    assert c.cmd("EXPIRE", "ttl", "600") == ("int", 1)
    assert c.cmd("PEXPIREAT", "soon", str(int(time.time() * 1000) - 1000)) \
        == ("int", 0)          # missing key: no-op
    assert c.cmd("SET", "soon", "gone") == ("simple", b"OK")
    assert c.cmd("PEXPIREAT", "soon", str(int(time.time() * 1000) - 1000)) \
        == ("int", 1)          # past timestamp deletes it
    assert c.cmd("SET", "d1", "1") == ("simple", b"OK")
    assert c.cmd("SET", "d2", "2") == ("simple", b"OK")
    assert c.cmd("DEL", "d1", "d2") == ("int", 2)
    assert c.cmd("SET", "zero", "1") == ("simple", b"OK")
    assert c.cmd("EXPIRE", "zero", "0") == ("int", 1)  # negative-TTL delete
    assert c.cmd("GET", "k042") == ("bulk", b"v042")
    c.close()
    stop_server(proc, hard=True)  # ---- SIGKILL: no clean close ----
    ok("loaded + SIGKILL")

    # ---- boot 2: crash recovery ----
    print("== boot 2: crash recovery")
    proc = start_server(port, wal_path)
    if proc is None:
        return 1
    c = Resp("127.0.0.1", port)
    for i in range(N):
        r = c.cmd("GET", f"k{i:03d}")
        if r != ("bulk", f"v{i:03d}".encode()):
            fail(f"key k{i:03d} lost after crash: {r}")
            break
    else:
        ok("all 200 SETs survived kill -9")
    assert c.cmd("GET", "ctr") == ("bulk", b"5"), c.cmd("GET", "ctr")
    ok("5 INCRs survived kill -9")
    assert c.cmd("GET", "ttl") == ("bulk", b"x")
    ok("EXPIRE'd key survived (absolute TTL)")
    assert c.cmd("GET", "d1") == ("nil", None)
    assert c.cmd("GET", "d2") == ("nil", None)
    ok("DEL survived kill -9")
    assert c.cmd("GET", "soon") == ("nil", None)
    ok("past-PEXPIREAT deletion survived")
    assert c.cmd("GET", "zero") == ("nil", None)
    ok("negative-TTL EXPIRE deletion survived")
    c.close()

    # Append a torn record (crash mid-append of a new SET).
    with open(wal_path, "ab") as f:
        f.write(b"*3\r\n$3\r\nset\r\n$3\r\nzzz\r\n$5\r\npar")
    stop_server(proc, hard=True)
    print("== boot 3: torn-tail truncation + append")
    proc = start_server(port, wal_path)
    if proc is None:
        return 1
    c = Resp("127.0.0.1", port)
    assert c.cmd("GET", "k000") == ("bulk", b"v000")
    assert c.cmd("GET", "zzz") == ("nil", None)   # torn record cut
    ok("torn tail truncated; prior data intact")
    assert c.cmd("SET", "zzz", "full") == ("simple", b"OK")
    assert c.cmd("GET", "zzz") == ("bulk", b"full")
    ok("WAL appendable after truncation")
    assert c.cmd("INFO")[0] == "bulk"
    c.close()
    stop_server(proc, hard=False)  # clean SIGTERM

    # ---- boot 4: clean shutdown then normal restart ----
    print("== boot 4: restart after clean shutdown")
    proc = start_server(port, wal_path)
    if proc is None:
        return 1
    c = Resp("127.0.0.1", port)
    assert c.cmd("GET", "k199") == ("bulk", b"v199")
    assert c.cmd("GET", "zzz") == ("bulk", b"full")
    c.close()
    stop_server(proc, hard=False)
    ok("clean-shutdown WAL replays on next boot")

    if os.path.exists(wal_path):
        os.unlink(wal_path)
    if failures:
        print(f"recovery: {failures} FAILURE(S)")
        return 1
    print("recovery: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
