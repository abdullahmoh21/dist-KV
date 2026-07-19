"""Phase 3 — SET options (reaper lock + dedup guard): NX / XX / EX / PX / GET.

Covers the two traps specific to this codebase:
  1. A no-op SET (NX on a present key, XX on an absent key) must NOT propagate.
     We assert value-unchanged AND that the AOF carries no phantom write.
  2. SET ... EX/PX rides the Phase-2c absolute-time rewrite: the propagated frame
     becomes `SET k v` + `PEXPIREAT k <abs-ms>`, so deadlines survive restart and
     the option tokens (NX/XX/GET/EX/PX) never reach the AOF verbatim.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, Checker, run_dir_for

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7080


def main():
    rd = run_dir_for("set_options")
    aof = os.path.join(rd, "appendonly.aof")
    os.makedirs(rd, exist_ok=True)
    if os.path.exists(aof):
        os.remove(aof)
    ck = Checker("set_options")

    p = start_server(BIN, rd, PORT)
    try:
        c = Client(PORT)
        # --- plain SET still works (regression) ---
        ck.check("plain SET", c.cmd("SET", "k", "v"), lambda r: r == "OK")
        ck.check("plain GET", c.cmd("GET", "k"), lambda r: r == "v")

        # --- NX: set only if absent ---
        ck.check("NX absent -> OK", c.cmd("SET", "nxk", "1", "NX"), lambda r: r == "OK")
        ck.check("NX created value", c.cmd("GET", "nxk"), lambda r: r == "1")
        ck.check("NX present -> nil", c.cmd("SET", "nxk", "2", "NX"), lambda r: r is None)
        ck.check("NX did not overwrite", c.cmd("GET", "nxk"), lambda r: r == "1")

        # --- XX: set only if present ---
        ck.check("XX absent -> nil", c.cmd("SET", "xxk", "1", "XX"), lambda r: r is None)
        ck.check("XX did not create", c.cmd("GET", "xxk"), lambda r: r is None)
        c.cmd("SET", "xxk", "1")
        ck.check("XX present -> OK", c.cmd("SET", "xxk", "2", "XX"), lambda r: r == "OK")
        ck.check("XX updated value", c.cmd("GET", "xxk"), lambda r: r == "2")

        # --- EX / PX: expiry ---
        ck.check("EX -> OK", c.cmd("SET", "exk", "v", "EX", "100"), lambda r: r == "OK")
        ck.check("EX TTL ~100", c.cmd("TTL", "exk"), lambda r: 95 <= int(r) <= 100)
        ck.check("PX -> OK", c.cmd("SET", "pxk", "v", "PX", "50000"), lambda r: r == "OK")
        ck.check("PX PTTL ~50000", c.cmd("PTTL", "pxk"), lambda r: 45000 < int(r) <= 50000)

        # plain SET (no KEEPTTL) clears an existing TTL
        c.cmd("SET", "exk", "v2")
        ck.check("plain SET clears TTL", c.cmd("PTTL", "exk"), lambda r: r == "-1")

        # --- combined NX + EX: the reaper lock ---
        ck.check("SET lock NX EX absent -> OK", c.cmd("SET", "lock:reaper", "1", "NX", "EX", "30"),
                 lambda r: r == "OK")
        ck.check("lock has TTL ~30", c.cmd("TTL", "lock:reaper"), lambda r: 25 <= int(r) <= 30)
        ck.check("second candidate NX loses -> nil", c.cmd("SET", "lock:reaper", "2", "NX", "EX", "30"),
                 lambda r: r is None)
        ck.check("lock value unchanged (winner keeps it)", c.cmd("GET", "lock:reaper"), lambda r: r == "1")

        # --- GET option: return old value ---
        c.cmd("SET", "g", "old")
        ck.check("SET GET returns old", c.cmd("SET", "g", "new", "GET"), lambda r: r == "old")
        ck.check("SET GET applied new", c.cmd("GET", "g"), lambda r: r == "new")
        ck.check("SET GET on absent -> nil", c.cmd("SET", "gmiss", "x", "GET"), lambda r: r is None)
        ck.check("SET GET created it", c.cmd("GET", "gmiss"), lambda r: r == "x")
        # NX + GET: report the old value but do NOT overwrite
        ck.check("NX GET present -> old", c.cmd("SET", "g", "newer", "NX", "GET"), lambda r: r == "new")
        ck.check("NX GET did not overwrite", c.cmd("GET", "g"), lambda r: r == "new")
        # GET on a wrong-type key errors and does not mutate
        c.cmd("ZADD", "zz", "1", "m")
        ck.check("SET GET wrongtype -> ERR", c.cmd("SET", "zz", "x", "GET"),
                 lambda r: isinstance(r, str) and ("WRONGTYPE" in r or r.startswith("ERR")))
        ck.check("SET GET wrongtype did not clobber", c.cmd("ZSCORE", "zz", "m"), lambda r: r == "1")

        # --- syntax / value errors ---
        ck.check("NX+XX -> syntax err", c.cmd("SET", "k", "v", "NX", "XX"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("EX+PX -> syntax err", c.cmd("SET", "k", "v", "EX", "1", "PX", "1"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("unknown opt -> syntax err", c.cmd("SET", "k", "v", "BOGUS"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("EX missing arg -> syntax err", c.cmd("SET", "k", "v", "EX"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("EX non-int -> err", c.cmd("SET", "k", "v", "EX", "abc"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("EX zero -> invalid expire", c.cmd("SET", "k", "v", "EX", "0"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("EX negative -> invalid expire", c.cmd("SET", "k", "v", "PX", "-5"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))

        # a failed-option SET must not mutate the key
        ck.check("k untouched after bad opts", c.cmd("GET", "k"), lambda r: r == "v")

        # let the 1s AOF flush timer sync before we snapshot the file on shutdown
        time.sleep(1.4)
        c.close()
    finally:
        stop_server(p)

    data = open(aof, "rb").read()

    # EX/PX rewrote to absolute PEXPIREAT; the relative option tokens never persist
    ck.check("AOF has PEXPIREAT (rewritten from EX/PX)", b"PEXPIREAT\r\n" in data, lambda r: r is True)
    ck.check("AOF has no EX option frame", b"\r\nEX\r\n" in data, lambda r: r is False)
    ck.check("AOF has no PX option frame", b"\r\nPX\r\n" in data, lambda r: r is False)
    ck.check("AOF has no NX option frame", b"\r\nNX\r\n" in data, lambda r: r is False)
    ck.check("AOF has no GET option frame", b"\r\nGET\r\n" in data, lambda r: r is False)
    # a no-op NX (loser) never wrote value "2"; the lock member must not appear as a stored value 2
    ck.check("no phantom write of NX-loser value", b"$1\r\n2\r\n$10\r\nlock:reaper" not in data,
             lambda r: r is True)

    # --- restart: absolute deadline + values survive AOF replay ---
    p = start_server(BIN, rd, PORT)
    try:
        c = Client(PORT)
        ck.check("lock TTL survives restart", int(c.cmd("TTL", "lock:reaper")), lambda r: 20 < r <= 30)
        ck.check("lock value survives restart", c.cmd("GET", "lock:reaper"), lambda r: r == "1")
        ck.check("pxk TTL survives restart", int(c.cmd("PTTL", "pxk")), lambda r: 40000 < r <= 50000)
        ck.check("xxk value survives restart", c.cmd("GET", "xxk"), lambda r: r == "2")
        ck.check("g value survives restart", c.cmd("GET", "g"), lambda r: r == "new")
        c.close()
    finally:
        stop_server(p)

    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
