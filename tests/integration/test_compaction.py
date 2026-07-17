"""Fork-based AOF compaction: bloat the AOF past the 2x-growth trigger, let the
child rewrite it, and assert live TTLs are re-emitted as PEXPIREAT and survive a
restart from the compacted snapshot. Drives aof_compact.c + aof_resp_encode.c."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, Checker, run_dir_for

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7050


def main():
    rd = run_dir_for("compaction")
    aof = os.path.join(rd, "appendonly.aof")
    os.makedirs(rd, exist_ok=True)
    for f in ("appendonly.aof", "srv.log", "compacted.aof", "tmp.aof"):
        fp = os.path.join(rd, f)
        if os.path.exists(fp):
            os.remove(fp)
    ck = Checker("compaction")

    p = start_server(BIN, rd, PORT)
    try:
        c = Client(PORT)
        # keys carrying live TTLs that must survive the rewrite
        c.cmd("SET", "ttlkey", "payload"); c.cmd("PEXPIRE", "ttlkey", "100000")
        c.cmd("ZADD", "ztll", "5", "m1"); c.cmd("PEXPIRE", "ztll", "100000")
        # a multi-member ZSET so compaction serializes a real ZADD batch.
        # NOTE: kept at 400 members deliberately. The compaction serializer emits
        # one ZADD frame per batch (up to ZSET_BATCH_SIZE=1000 members -> 2002
        # args), but the RESP parser caps an array at MAX_ARGS=1024 on reload, so
        # a larger set would produce an AOF that cannot be replayed. The >1000
        # batch-flush write path is covered separately by the aof_compact unit
        # test (which checks the serialized bytes without reloading them).
        for base in range(0, 400, 100):
            args = []
            for i in range(base, base + 100):
                args += [str(i), f"m{i}"]
            c.cmd("ZADD", "bigz", *args)
        # bloat AOF > 64KB to trip the 2x-growth compaction fork
        val = "x" * 200
        for i in range(1200):
            c.cmd("SET", f"k{i}", val)

        # wait for the event loop to fork + finish compaction
        done = False
        deadline = time.time() + 10
        while time.time() < deadline:
            time.sleep(0.5)
            log = open(os.path.join(rd, "srv.log")).read()
            if "compaction complete" in log:
                done = True
                break
        ck.check("compaction ran", done, lambda r: r is True)
        ck.check("ttlkey PTTL live pre-restart", int(c.cmd("PTTL", "ttlkey")), lambda r: 90000 < r <= 100000)
        c.close()
    finally:
        stop_server(p)

    data = open(aof, "rb").read()
    ck.check("compacted AOF re-emits PEXPIREAT", data.count(b"PEXPIREAT\r\n"), lambda n: n >= 2)

    # restart from the compacted snapshot: TTLs + data survive
    p = start_server(BIN, rd, PORT)
    c = Client(PORT)
    ck.check("ttlkey survives compaction+restart", int(c.cmd("PTTL", "ttlkey")), lambda r: 85000 < r <= 100000)
    ck.check("ztll survives compaction+restart", int(c.cmd("PTTL", "ztll")), lambda r: 85000 < r <= 100000)
    ck.check("ztll member intact", c.cmd("ZSCORE", "ztll", "m1"), lambda r: r == "5")
    ck.check("a bloat key intact", c.cmd("GET", "k500"), lambda r: r == "x" * 200)
    c.close()
    stop_server(p)
    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
