"""Shared RESP + server-process helpers for dist-KV integration tests.

Each test module imports this, starts its own server_cov instance in a temp
run dir, and drives it over a socket. The instrumented binary writes coverage
profiles via the inherited LLVM_PROFILE_FILE env var (per-pid pattern), so
forked compaction children and replica processes each emit their own profraw.
"""
import socket, subprocess, os, signal, time, sys


def enc(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def _read(s, buf):
    while b"\r\n" not in buf[0]:
        chunk = s.recv(65536)
        if not chunk:
            raise ConnectionError("server closed")
        buf[0] += chunk
    line, _, rest = buf[0].partition(b"\r\n")
    buf[0] = rest
    t, v = line[:1], line[1:]
    if t in (b"+", b"-", b":"):
        return v.decode()
    if t == b"$":
        n = int(v)
        if n == -1:
            return None
        while len(buf[0]) < n + 2:
            buf[0] += s.recv(65536)
        d = buf[0][:n]
        buf[0] = buf[0][n + 2:]
        return d.decode()
    if t == b"*":
        n = int(v)
        return None if n == -1 else [_read(s, buf) for _ in range(n)]
    return ("?", line)


class Client:
    def __init__(self, port, timeout=5):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.settimeout(timeout)
        self.buf = [b""]

    def cmd(self, *args):
        self.s.sendall(enc(*args))
        return _read(self.s, self.buf)

    def send_raw(self, data):
        self.s.sendall(data)

    def read(self):
        return _read(self.s, self.buf)

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def start_server(binary, run_dir, port, extra=None):
    os.makedirs(run_dir, exist_ok=True)
    binary = os.path.abspath(binary)  # cwd=run_dir, so a relative path would miss
    args = [binary, "--port", str(port)] + (extra or [])
    p = subprocess.Popen(
        args, cwd=run_dir,
        stdout=open(os.path.join(run_dir, "srv.log"), "a"),
        stderr=subprocess.STDOUT,
    )
    # wait for the port to accept connections
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return p
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not come up on port %d" % port)


def launch_and_wait(binary, run_dir, extra=None, timeout=5):
    """Start the server and wait for it to exit on its own (used for fatal-startup
    paths like a corrupt AOF). Returns the exit code. Coverage is still recorded
    via continuous mode even though the process terminates."""
    os.makedirs(run_dir, exist_ok=True)
    binary = os.path.abspath(binary)
    p = subprocess.Popen(
        [binary, "--port", str(0)] + (extra or []), cwd=run_dir,
        stdout=open(os.path.join(run_dir, "srv.log"), "a"),
        stderr=subprocess.STDOUT,
    )
    try:
        return p.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.send_signal(signal.SIGINT)
        try:
            return p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
            return -1


def stop_server(p):
    p.send_signal(signal.SIGINT)
    try:
        p.wait(timeout=4)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


class Checker:
    def __init__(self, name):
        self.name = name
        self.fails = 0
        self.count = 0

    def check(self, label, got, pred):
        self.count += 1
        try:
            ok = pred(got)
        except Exception as e:  # noqa: BLE001
            ok = False
            got = "%r (pred raised %s)" % (got, e)
        status = "PASS" if ok else "FAIL"
        if not ok:
            self.fails += 1
        print(f"  {status}  {label}: {got!r}")

    def done(self):
        tag = "ALL PASS" if self.fails == 0 else f"{self.fails} FAILURES"
        print(f"[{self.name}] {tag} ({self.count} checks)\n")
        return self.fails


def run_dir_for(name):
    base = os.environ.get("DKV_TEST_RUNDIR", "/tmp/dkv_test")
    d = os.path.join(base, name)
    return d
