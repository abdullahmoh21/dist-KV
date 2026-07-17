"""Phase 4 — BZPOPMIN blocking claim (kills worker polling).

Covers the traps specific to this codebase:
  1. The raw `BZPOPMIN q 0` frame must NEVER reach the AOF. Replaying it would
     park a phantom client, and aof_load treats a non-EE_WRITE_OK replay as a
     fatal error. Every served pop propagates a rewritten `ZPOPMIN key` frame.
  2. A pop served from the *parked* path happens outside handle_client_read's
     EE_WRITE_OK gate, so it must synthesize and propagate its own frame
     (the active_expire_del_cb pattern). We assert this by restarting: a job
     claimed by a blocked worker must stay claimed after AOF replay.
  3. A BZPOPMIN that times out wrote nothing and must not propagate.
"""
import sys, os, time, select
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, enc, start_server, stop_server, Checker, run_dir_for

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7090


def quiet(c, secs=0.3):
    """True if the client received nothing within `secs` (i.e. it is still parked)."""
    if c.buf[0]:
        return False
    r, _, _ = select.select([c.s], [], [], secs)
    return not r


def main():
    rd = run_dir_for("bzpopmin")
    aof = os.path.join(rd, "appendonly.aof")
    os.makedirs(rd, exist_ok=True)
    if os.path.exists(aof):
        os.remove(aof)
    ck = Checker("bzpopmin")

    p = start_server(BIN, rd, PORT)
    try:
        c = Client(PORT)

        # --- immediate serve: data already there, no parking ---
        c.cmd("ZADD", "q", "1", "j1")
        ck.check("immediate pop -> [key, member, score]", c.cmd("BZPOPMIN", "q", "0"),
                 lambda r: r == ["q", "j1", "1"])
        ck.check("emptied zset key deleted", c.cmd("ZSCORE", "q", "j1"), lambda r: r is None)

        # --- multi-key: scans keys left to right ---
        c.cmd("ZADD", "kb", "5", "fromb")
        ck.check("multi-key serves the populated one", c.cmd("BZPOPMIN", "ka", "kb", "0"),
                 lambda r: r == ["kb", "fromb", "5"])
        c.cmd("ZADD", "ka", "9", "froma")
        c.cmd("ZADD", "kb", "1", "fromb2")
        ck.check("leftmost key wins even with a worse score",
                 c.cmd("BZPOPMIN", "ka", "kb", "0"), lambda r: r == ["ka", "froma", "9"])
        ck.check("kb untouched by leftmost win", c.cmd("ZSCORE", "kb", "fromb2"), lambda r: r == "1")
        c.cmd("BZPOPMIN", "kb", "0")

        # --- lowest score wins within a key ---
        c.cmd("ZADD", "pq", "10", "low-prio")
        c.cmd("ZADD", "pq", "2", "high-prio")
        ck.check("min score served first", c.cmd("BZPOPMIN", "pq", "0"),
                 lambda r: r == ["pq", "high-prio", "2"])
        c.cmd("BZPOPMIN", "pq", "0")

        # --- timeout: park, then give up with a null array ---
        t0 = time.time()
        ck.check("timeout -> null array", c.cmd("BZPOPMIN", "empty", "1"), lambda r: r is None)
        ck.check("timeout actually waited ~1s", time.time() - t0, lambda d: 0.9 <= d < 2.0)

        # fractional timeout
        t0 = time.time()
        ck.check("fractional timeout -> null array", c.cmd("BZPOPMIN", "empty", "0.3"),
                 lambda r: r is None)
        ck.check("fractional timeout waited ~0.3s", time.time() - t0, lambda d: 0.2 <= d < 1.0)

        # --- the real thing: block, then a producer wakes us ---
        w1 = Client(PORT, timeout=5)
        w1.send_raw(enc("BZPOPMIN", "bq", "0"))
        ck.check("worker parks (no reply yet)", quiet(w1), lambda r: r is True)
        c.cmd("ZADD", "bq", "5", "job-a")
        ck.check("blocked worker woken by ZADD", w1.read(), lambda r: r == ["bq", "job-a", "5"])
        ck.check("woken pop removed the job", c.cmd("ZSCORE", "bq", "job-a"), lambda r: r is None)

        # --- exactly one waker; arrival order decides ---
        w1.send_raw(enc("BZPOPMIN", "fair", "0"))
        time.sleep(0.15)  # guarantee w1 parks first
        w2 = Client(PORT, timeout=5)
        w2.send_raw(enc("BZPOPMIN", "fair", "0"))
        time.sleep(0.15)
        c.cmd("ZADD", "fair", "1", "only-job")
        ck.check("first parked worker wins", w1.read(), lambda r: r == ["fair", "only-job", "1"])
        ck.check("second worker still parked", quiet(w2), lambda r: r is True)
        c.cmd("ZADD", "fair", "2", "second-job")
        ck.check("second worker wakes on next job", w2.read(),
                 lambda r: r == ["fair", "second-job", "2"])
        w2.close()

        # --- a parked client that disconnects must not wedge the server ---
        w3 = Client(PORT, timeout=5)
        w3.send_raw(enc("BZPOPMIN", "ghost", "0"))
        time.sleep(0.2)
        w3.close()
        time.sleep(0.2)
        c.cmd("ZADD", "ghost", "1", "orphan")
        ck.check("job survives a vanished parked client",
                 c.cmd("BZPOPMIN", "ghost", "0"), lambda r: r == ["ghost", "orphan", "1"])
        ck.check("server alive after parked-client disconnect", c.cmd("PING"), lambda r: r == "PONG")

        # --- errors: must reply immediately, never park ---
        ck.check("no timeout arg -> arity err", c.cmd("BZPOPMIN", "k"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("bare BZPOPMIN -> arity err", c.cmd("BZPOPMIN"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("non-numeric timeout -> err", c.cmd("BZPOPMIN", "k", "abc"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("negative timeout -> err", c.cmd("BZPOPMIN", "k", "-1"),
                 lambda r: isinstance(r, str) and r.startswith("ERR"))
        c.cmd("SET", "strk", "v")
        ck.check("wrongtype -> err (does not park)", c.cmd("BZPOPMIN", "strk", "0"),
                 lambda r: isinstance(r, str) and ("WRONGTYPE" in r or r.startswith("ERR")))
        ck.check("wrongtype key not clobbered", c.cmd("GET", "strk"), lambda r: r == "v")
        ck.check("server still responsive after errors", c.cmd("PING"), lambda r: r == "PONG")

        # --- leave durable state for the restart assertions ---
        # `keep` retains one job; `claimed` is emptied by a *blocked* worker, so
        # its pop only persists if the parked-serve path propagated.
        c.cmd("ZADD", "keep", "7", "still-queued")
        w4 = Client(PORT, timeout=5)
        w4.send_raw(enc("BZPOPMIN", "claimed", "0"))
        time.sleep(0.2)
        c.cmd("ZADD", "claimed", "3", "taken-while-blocked")
        ck.check("blocked worker claimed the durable job", w4.read(),
                 lambda r: r == ["claimed", "taken-while-blocked", "3"])
        w4.close()

        time.sleep(1.4)  # let the 1s AOF flush timer sync before shutdown
        c.close()
        w1.close()
    finally:
        stop_server(p)

    data = open(aof, "rb").read()

    # The blocking frame must never persist — replay would park a phantom client.
    ck.check("AOF has no BZPOPMIN frame", b"BZPOPMIN" in data, lambda r: r is False)
    # ...it is rewritten to the plain, replayable pop.
    ck.check("AOF has rewritten ZPOPMIN frame", b"\r\n$7\r\nZPOPMIN\r\n" in data, lambda r: r is True)
    # A timed-out BZPOPMIN wrote nothing; "empty" must never appear as a popped key.
    ck.check("timed-out pop propagated nothing",
             b"$7\r\nZPOPMIN\r\n$5\r\nempty\r\n" not in data, lambda r: r is True)
    # The parked-serve path propagated its own frame.
    ck.check("parked-serve pop reached the AOF",
             b"$7\r\nZPOPMIN\r\n$7\r\nclaimed\r\n" in data, lambda r: r is True)

    # --- restart: AOF replays without wedging, state reconstructs ---
    p = start_server(BIN, rd, PORT)
    try:
        c = Client(PORT)
        ck.check("server replayed AOF and is up", c.cmd("PING"), lambda r: r == "PONG")
        ck.check("unclaimed job survives restart", c.cmd("ZSCORE", "keep", "still-queued"),
                 lambda r: r == "7")
        ck.check("job claimed while blocked stays claimed",
                 c.cmd("ZSCORE", "claimed", "taken-while-blocked"), lambda r: r is None)
        ck.check("immediate-pop job stays popped", c.cmd("ZSCORE", "q", "j1"), lambda r: r is None)
        ck.check("BZPOPMIN works after restart", c.cmd("BZPOPMIN", "keep", "0"),
                 lambda r: r == ["keep", "still-queued", "7"])
        c.close()
    finally:
        stop_server(p)

    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
