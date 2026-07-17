"""Leader-follower replication: a replica connects with --replicaof, receives
the master's snapshot (PSYNC full sync), then mirrors streamed writes. Drives
replication.c on both sides plus the snapshot-fork path."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, Checker, run_dir_for

BIN = sys.argv[1]
MPORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7020
RPORT = MPORT + 1


def wait_until(fn, timeout=6):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.1)
    return False


def main():
    mrd = run_dir_for("repl_master")
    rrd = run_dir_for("repl_replica")
    ck = Checker("replication")
    master = start_server(BIN, mrd, MPORT)
    replica = None
    try:
        m = Client(MPORT)
        # pre-load data BEFORE the replica connects -> must arrive via snapshot
        for i in range(20):
            m.cmd("SET", f"pre{i}", f"v{i}")
        m.cmd("ZADD", "zpre", "1", "a", "2", "b")

        replica = start_server(BIN, rrd, RPORT, extra=["--replicaof", "127.0.0.1", str(MPORT)])
        r = Client(RPORT)

        # snapshot sync: replica should eventually hold the pre-loaded keys
        ok = wait_until(lambda: r.cmd("GET", "pre0") == "v0")
        ck.check("snapshot: replica got pre0", ok, lambda x: x is True)
        ck.check("snapshot: replica got pre19", r.cmd("GET", "pre19"), lambda x: x == "v19")
        ck.check("snapshot: zset replicated", r.cmd("ZSCORE", "zpre", "b"), lambda x: x == "2")

        # streaming: new writes on master propagate to replica
        m.cmd("SET", "live1", "hello")
        ok = wait_until(lambda: r.cmd("GET", "live1") == "hello")
        ck.check("stream: SET propagated", ok, lambda x: x is True)

        m.cmd("INCR", "counter")
        m.cmd("INCR", "counter")
        ok = wait_until(lambda: r.cmd("GET", "counter") == "2")
        ck.check("stream: INCR propagated", ok, lambda x: x is True)

        m.cmd("ZADD", "zlive", "7", "m")
        ok = wait_until(lambda: r.cmd("ZSCORE", "zlive", "m") == "7")
        ck.check("stream: ZADD propagated", ok, lambda x: x is True)

        m.cmd("DEL", "live1")
        ok = wait_until(lambda: r.cmd("GET", "live1") is None)
        ck.check("stream: DEL propagated", ok, lambda x: x is True)

        # EXPIRE propagates as absolute deadline -> replica converges on TTL
        m.cmd("SET", "tk", "v"); m.cmd("EXPIRE", "tk", "1000")
        ok = wait_until(lambda: r.cmd("PTTL", "tk") not in (None, "-2", "-1"))
        ck.check("stream: EXPIRE (as PEXPIREAT) propagated", ok, lambda x: x is True)

        # WAIT with the replica synced returns quickly
        ck.check("WAIT 1 2000 -> >=1", m.cmd("WAIT", "1", "2000"), lambda x: int(x) >= 1)

        # a second replica also snapshots + streams
        r2rd = run_dir_for("repl_replica2")
        replica2 = start_server(BIN, r2rd, RPORT + 1, extra=["--replicaof", "127.0.0.1", str(MPORT)])
        try:
            r2 = Client(RPORT + 1)
            ok = wait_until(lambda: r2.cmd("GET", "pre0") == "v0")
            ck.check("replica2 snapshot", ok, lambda x: x is True)
            m.cmd("SET", "both", "yes")
            ok = wait_until(lambda: r.cmd("GET", "both") == "yes" and r2.cmd("GET", "both") == "yes")
            ck.check("both replicas receive stream", ok, lambda x: x is True)
            # WAIT for 2 replicas
            ck.check("WAIT 2 2000 -> >=2", m.cmd("WAIT", "2", "2000"), lambda x: int(x) >= 2)

            # Pump > 1MB (backlog cap) of pipelined writes so the replication
            # backlog ring buffer wraps around, then confirm the replica caught up.
            from harness import enc
            val = "p" * 200
            payload = b"".join(enc("SET", f"pump{i}", val) for i in range(7000))
            m.send_raw(payload)
            for _ in range(7000):
                m.read()
            ck.check("ring-wrap: last pumped key on master", m.cmd("GET", "pump6999"), lambda x: x == val)
            ok = wait_until(lambda: r.cmd("GET", "pump6999") == val, timeout=8)
            ck.check("ring-wrap: replica caught up after wraparound", ok, lambda x: x is True)
            r2.close()
        finally:
            stop_server(replica2)

        # WAIT for more replicas than exist parks then times out to the real
        # count. Generous timeout so the replica's periodic ACK catches up to the
        # offset left by the ring-wrap pump above.
        ck.check("WAIT 5 3000 times out to count", m.cmd("WAIT", "5", "3000"), lambda x: int(x) >= 1)

        # A single value larger than the whole 1MB backlog exercises the
        # "command bigger than backlog" branch. By design this evicts streaming
        # replicas (they can't be served incrementally from the backlog), so we
        # only assert the master applied it — the code path is what we're after.
        huge = "H" * (1024 * 1024 + 500)
        ck.check("oversized SET on master", m.cmd("SET", "huge", huge), lambda x: x == "OK")
        ck.check("master serves oversized value", len(m.cmd("GET", "huge")), lambda n: n == len(huge))

        r.close()
        m.close()
    finally:
        if replica:
            stop_server(replica)
        stop_server(master)
    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
