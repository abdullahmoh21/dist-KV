"""Phase 5 — end-to-end at-least-once job-queue integration.

The server ships NO queue code: the broker is just the primitives (ZPOPMIN /
BZPOPMIN atomic claim, SET NX/PX lock + dedup, TTL + active expiry). The
reaper and visibility-timeout logic live entirely client-side — exactly the
design in notes/TODO_QUEUE.md. This test *is* that client, proving the
primitives compose into at-least-once delivery.

Three scenarios, each with its own server lifecycle:
  A. reaper e2e     — a worker dies mid-job; the reaper reclaims exactly its
                      job after the visibility timeout, no loss / no double.
  B. AOF hard-crash — SIGKILL (not clean shutdown) then replay reconstructs
                      queue + processing keys + absolute TTLs.
  C. replica mirror — a follower converges on queue state and processing TTLs
                      with the correct absolute deadline.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, Checker, run_dir_for

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7080


def wait_until(fn, timeout=6):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.05)
    return False


# --- the client-side queue vocabulary (this is the whole "broker protocol") ---

def enqueue(c, member, score):
    # priority/FIFO ordering by score; member is the job id
    c.cmd("ZADD", "q", str(score), member)


def claim(c, worker, vis_ms):
    """Atomic claim via ZPOPMIN, then register the in-flight job and stamp a
    processing:<id> key whose TTL is the visibility timeout. Returns the job id
    or None if the queue is empty."""
    res = c.cmd("ZPOPMIN", "q")          # flat [member, score] or []
    if not res:
        return None
    jid = res[0]
    c.cmd("ZADD", "inflight", "0", jid)  # enumerable set of in-flight ids
    c.cmd("SET", f"processing:{jid}", worker, "PX", str(vis_ms))
    return jid


def complete(c, jid):
    """Ack a job. The NX dedup guard makes the *effect* idempotent even if the
    job ran twice (at-least-once): only the first completion returns OK."""
    first = c.cmd("SET", f"done:{jid}", "1", "NX")
    c.cmd("ZREM", "inflight", jid)
    c.cmd("DEL", f"processing:{jid}")
    return first == "OK"


def reaper_sweep(c):
    """One reaper pass. Acquire the leader lock; for every in-flight job whose
    processing:<id> key has expired (worker presumed dead), re-enqueue it unless
    it already completed. Returns the list of reclaimed job ids."""
    if c.cmd("SET", "lock:reaper", "1", "NX", "EX", "5") != "OK":
        return []  # another reaper holds the lock
    reclaimed = []
    for jid in (c.cmd("ZRANGE", "inflight", "0", "-1") or []):
        if c.cmd("GET", f"processing:{jid}") is None:      # visibility timeout elapsed
            c.cmd("ZREM", "inflight", jid)
            if c.cmd("GET", f"done:{jid}") is None:        # not already acked
                c.cmd("ZADD", "q", "0", jid)               # re-enqueue at head
                reclaimed.append(jid)
    c.cmd("DEL", "lock:reaper")
    return reclaimed


# --- Scenario A: reaper reclaims a dead worker's job ---------------------------

def scenario_reaper(ck):
    rd = run_dir_for("queue_reaper")
    aof = os.path.join(rd, "appendonly.aof")
    os.makedirs(rd, exist_ok=True)
    if os.path.exists(aof):
        os.remove(aof)
    p = start_server(BIN, rd, PORT)
    try:
        c = Client(PORT)
        VIS = 400  # ms visibility timeout

        # enqueue 5 jobs with unique ids allocated atomically via INCR
        jobs = []
        for i in range(5):
            jid = c.cmd("INCR", "jobs:next-id")   # "1".."5"
            member = f"job-{jid}"
            enqueue(c, member, i)
            jobs.append(member)
        ck.check("5 jobs queued", c.cmd("ZRANGE", "q", "0", "-1"), lambda r: len(r) == 5)

        # worker-1 claims the head job then DIES (never acks) — its processing
        # key will expire, exposing the job to the reaper.
        dead = claim(c, "worker-1", VIS)
        ck.check("worker-1 claimed head job", dead, lambda r: r == "job-1")

        # workers 2..5 claim and complete the rest cleanly
        healthy_done = []
        for w in range(2, 6):
            jid = claim(c, f"worker-{w}", VIS)
            if jid and complete(c, jid):
                healthy_done.append(jid)
        ck.check("4 healthy jobs completed", len(healthy_done), lambda n: n == 4)
        ck.check("queue drained", c.cmd("ZRANGE", "q", "0", "-1"), lambda r: r == [])
        ck.check("only dead job still in-flight",
                 c.cmd("ZRANGE", "inflight", "0", "-1"), lambda r: r == ["job-1"])

        # before the visibility timeout, a reaper sweep must NOT reclaim it
        ck.check("no premature reclaim", reaper_sweep(c), lambda r: r == [])

        # wait past the visibility timeout: processing:job-1 must vanish (active
        # expiry reaps it even with nothing touching the key)
        ok = wait_until(lambda: c.cmd("GET", "processing:job-1") is None, timeout=3)
        ck.check("processing key expired at deadline", ok, lambda x: x is True)

        # now the reaper reclaims exactly the dead job — and nothing else
        reclaimed = reaper_sweep(c)
        ck.check("reaper reclaimed exactly job-1", reclaimed, lambda r: r == ["job-1"])
        ck.check("job-1 re-enqueued", c.cmd("ZRANGE", "q", "0", "-1"), lambda r: r == ["job-1"])
        ck.check("dead job never marked done (no double-complete)",
                 c.cmd("GET", "done:job-1"), lambda r: r is None)

        # a healthy worker now re-runs the reclaimed job to completion
        again = claim(c, "worker-6", VIS)
        ck.check("job-1 re-claimed", again, lambda r: r == "job-1")
        ck.check("job-1 completes on retry", complete(c, again), lambda r: r is True)

        # at-least-once holds: every original job completed exactly once
        done_count = sum(1 for j in jobs if c.cmd("GET", f"done:{j}") == "1")
        ck.check("all 5 jobs completed exactly once", done_count, lambda n: n == 5)
        ck.check("queue empty at end", c.cmd("ZRANGE", "q", "0", "-1"), lambda r: r == [])
        ck.check("inflight empty at end", c.cmd("ZRANGE", "inflight", "0", "-1"), lambda r: r == [])

        # reaper leader election: two candidates race the lock, exactly one wins
        c.cmd("DEL", "lock:reaper")
        c2 = Client(PORT)
        a = c.cmd("SET", "lock:reaper", "1", "NX", "EX", "30")
        b = c2.cmd("SET", "lock:reaper", "1", "NX", "EX", "30")
        ck.check("exactly one reaper wins the lock",
                 (a, b), lambda t: sorted([t[0], "OK" if t[1] == "OK" else "NULL"]) == ["NULL", "OK"])
        c2.close()
        c.close()
    finally:
        stop_server(p)


# --- Scenario B: AOF survives a hard crash (SIGKILL) --------------------------

def scenario_aof_crash(ck):
    rd = run_dir_for("queue_aof")
    aof = os.path.join(rd, "appendonly.aof")
    os.makedirs(rd, exist_ok=True)
    if os.path.exists(aof):
        os.remove(aof)

    p = start_server(BIN, rd, PORT)
    c = Client(PORT)
    # 3 jobs queued; claim 1 into processing with a long visibility TTL
    for i in range(3):
        enqueue(c, f"job-{i}", i)
    claimed = claim(c, "worker-A", 100000)      # PX 100s -> must survive as absolute
    ck.check("claimed job-0", claimed, lambda r: r == "job-0")
    pttl_before = int(c.cmd("PTTL", f"processing:{claimed}"))
    ck.check("processing TTL live pre-crash", pttl_before, lambda n: 95000 < n <= 100000)
    c.close()

    # let the 1s AOF flush timer fsync everything, THEN hard-kill (SIGKILL):
    # no clean shutdown, no atexit flush — pure replay-from-disk recovery.
    time.sleep(2.0)
    p.kill()
    p.wait()

    # replay
    p = start_server(BIN, rd, PORT)
    c = Client(PORT)
    ck.check("queue survived crash", c.cmd("ZRANGE", "q", "0", "-1"),
             lambda r: r == ["job-1", "job-2"])
    ck.check("in-flight set survived crash", c.cmd("ZRANGE", "inflight", "0", "-1"),
             lambda r: r == ["job-0"])
    ck.check("processing key survived crash", c.cmd("GET", "processing:job-0"),
             lambda r: r == "worker-A")
    pttl_after = int(c.cmd("PTTL", "processing:job-0"))
    ck.check("processing TTL is absolute (survived as deadline, not reset)",
             pttl_after, lambda n: 90000 < n <= 100000)
    c.close()
    stop_server(p)


# --- Scenario C: a replica mirrors queue state + absolute TTLs -----------------

def scenario_replica(ck):
    mrd = run_dir_for("queue_repl_master")
    rrd = run_dir_for("queue_repl_replica")
    master = start_server(BIN, mrd, PORT)
    replica = None
    try:
        m = Client(PORT)
        for i in range(4):
            enqueue(m, f"job-{i}", i)

        replica = start_server(BIN, rrd, PORT + 1, extra=["--replicaof", "127.0.0.1", str(PORT)])
        r = Client(PORT + 1)

        # snapshot: replica converges on the pre-loaded queue
        ok = wait_until(lambda: r.cmd("ZRANGE", "q", "0", "-1") == ["job-0", "job-1", "job-2", "job-3"])
        ck.check("replica mirrors queue via snapshot", ok, lambda x: x is True)

        # streamed atomic claim: ZPOPMIN effect propagates (job-0 removed everywhere)
        jid = claim(m, "worker-A", 100000)
        ck.check("master claimed job-0", jid, lambda r_: r_ == "job-0")
        ok = wait_until(lambda: r.cmd("ZSCORE", "q", "job-0") is None
                        and r.cmd("ZRANGE", "q", "0", "-1") == ["job-1", "job-2", "job-3"])
        ck.check("replica reflects the claim (ZPOPMIN streamed)", ok, lambda x: x is True)

        # the processing TTL replicates as an absolute deadline: the replica's
        # PTTL tracks the master's, it isn't reset to a fresh 100s on receipt.
        ok = wait_until(lambda: r.cmd("GET", "processing:job-0") == "worker-A")
        ck.check("processing key replicated", ok, lambda x: x is True)
        mp = int(m.cmd("PTTL", "processing:job-0"))
        rp = int(r.cmd("PTTL", "processing:job-0"))
        ck.check("replica TTL is a mirrored absolute deadline (near master's, not reset)",
                 (mp, rp), lambda t: t[1] > 0 and abs(t[0] - t[1]) < 2000)

        # blocking claim on the master serves immediately (queue non-empty) and
        # streams its rewritten ZPOPMIN effect to the replica too
        res = m.cmd("BZPOPMIN", "q", "0")
        ck.check("master BZPOPMIN served immediately", res, lambda x: x and x[0] == "q" and x[1] == "job-1")
        ok = wait_until(lambda: r.cmd("ZSCORE", "q", "job-1") is None)
        ck.check("replica reflects BZPOPMIN claim", ok, lambda x: x is True)

        r.close()
        m.close()
    finally:
        if replica:
            stop_server(replica)
        stop_server(master)


def main():
    ck = Checker("queue")
    scenario_reaper(ck)
    scenario_aof_crash(ck)
    scenario_replica(ck)
    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
