"""TTL lifecycle end-to-end: EXPIRE/PEXPIRE/PEXPIREAT/TTL/PTTL/PERSIST, plus
lazy expiry (delete-on-access) and SET-clears-TTL, against a live server."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, Checker, run_dir_for

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7030


def main():
    rd = run_dir_for("expiry")
    p = start_server(BIN, rd, PORT)
    ck = Checker("expiry")
    try:
        c = Client(PORT)

        ck.check("SET k v", c.cmd("SET", "k", "v"), lambda r: r == "OK")
        ck.check("PTTL no-ttl -> -1", c.cmd("PTTL", "k"), lambda r: r == "-1")
        ck.check("TTL no-ttl -> -1", c.cmd("TTL", "k"), lambda r: r == "-1")
        ck.check("PEXPIRE 5000", c.cmd("PEXPIRE", "k", "5000"), lambda r: r == "1")
        ck.check("PTTL ~5000", c.cmd("PTTL", "k"), lambda r: 4000 < int(r) <= 5000)
        ck.check("TTL ~5", c.cmd("TTL", "k"), lambda r: r == "5")
        ck.check("PERSIST -> 1", c.cmd("PERSIST", "k"), lambda r: r == "1")
        ck.check("PTTL after persist -> -1", c.cmd("PTTL", "k"), lambda r: r == "-1")
        ck.check("PERSIST again -> 0", c.cmd("PERSIST", "k"), lambda r: r == "0")

        # SET clears TTL
        c.cmd("PEXPIRE", "k", "9000")
        c.cmd("SET", "k", "v2")
        ck.check("SET clears TTL", c.cmd("PTTL", "k"), lambda r: r == "-1")

        # EXPIRE (relative seconds) works and rounds
        c.cmd("SET", "e", "v")
        ck.check("EXPIRE 100", c.cmd("EXPIRE", "e", "100"), lambda r: r == "1")
        ck.check("TTL ~100", c.cmd("TTL", "e"), lambda r: 95 <= int(r) <= 100)

        # PEXPIREAT absolute
        import time as _t
        abs_ms = int(_t.time() * 1000) + 60000
        c.cmd("SET", "pa", "v")
        ck.check("PEXPIREAT", c.cmd("PEXPIREAT", "pa", str(abs_ms)), lambda r: r == "1")
        ck.check("PTTL ~60000", c.cmd("PTTL", "pa"), lambda r: 55000 < int(r) <= 60000)

        # missing-key semantics
        ck.check("TTL missing -> -2", c.cmd("TTL", "nope"), lambda r: r == "-2")
        ck.check("EXPIRE missing -> 0", c.cmd("EXPIRE", "nope", "10"), lambda r: r == "0")

        # lazy expiry
        c.cmd("SET", "lz", "v")
        c.cmd("PEXPIRE", "lz", "150")
        time.sleep(0.3)
        ck.check("lazy GET -> nil", c.cmd("GET", "lz"), lambda r: r is None)
        ck.check("lazy PTTL -> -2", c.cmd("PTTL", "lz"), lambda r: r == "-2")

        # immediate/negative expiry
        c.cmd("SET", "neg", "v")
        ck.check("PEXPIRE -100 -> 1", c.cmd("PEXPIRE", "neg", "-100"), lambda r: r == "1")
        ck.check("neg gone", c.cmd("GET", "neg"), lambda r: r is None)

        # ZSET honors TTL
        c.cmd("ZADD", "zq", "1", "job1")
        c.cmd("PEXPIRE", "zq", "150")
        ck.check("ZSCORE live", c.cmd("ZSCORE", "zq", "job1"), lambda r: r == "1")
        time.sleep(0.3)
        ck.check("ZSCORE expired -> nil", c.cmd("ZSCORE", "zq", "job1"), lambda r: r is None)

        c.close()
    finally:
        stop_server(p)
    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
