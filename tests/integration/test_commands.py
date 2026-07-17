"""End-to-end command-surface coverage: KV, ZSET, misc, errors, pipelining,
multi-client, buffer growth, and client disconnect. Drives server.c + the
event loop + every handler over a real socket."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, Checker, run_dir_for, enc

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7010


def main():
    rd = run_dir_for("commands")
    p = start_server(BIN, rd, PORT)
    ck = Checker("commands")
    try:
        c = Client(PORT)

        # --- KV basics ---
        ck.check("PING", c.cmd("PING"), lambda r: r == "PONG")
        ck.check("SET", c.cmd("SET", "k", "v"), lambda r: r == "OK")
        ck.check("GET", c.cmd("GET", "k"), lambda r: r == "v")
        ck.check("GET missing", c.cmd("GET", "nope"), lambda r: r is None)
        ck.check("DEL", c.cmd("DEL", "k"), lambda r: r == "1")
        ck.check("DEL missing", c.cmd("DEL", "k"), lambda r: r == "0")

        # --- counters ---
        ck.check("INCR new", c.cmd("INCR", "n"), lambda r: r == "1")
        ck.check("INCRBY", c.cmd("INCRBY", "n", "41"), lambda r: r == "42")
        ck.check("DECR", c.cmd("DECR", "n"), lambda r: r == "41")
        ck.check("DECRBY", c.cmd("DECRBY", "n", "40"), lambda r: r == "1")
        c.cmd("SET", "notint", "abc")
        ck.check("INCR non-int errors", c.cmd("INCR", "notint"), lambda r: isinstance(r, str) and r.startswith("ERR"))

        # --- ZSET ---
        ck.check("ZADD 3", c.cmd("ZADD", "z", "3", "c", "1", "a", "2", "b"), lambda r: r == "3")
        ck.check("ZADD update", c.cmd("ZADD", "z", "5", "a"), lambda r: r == "0")
        ck.check("ZSCORE", c.cmd("ZSCORE", "z", "b"), lambda r: r == "2")
        ck.check("ZSCORE missing", c.cmd("ZSCORE", "z", "x"), lambda r: r is None)
        ck.check("ZRANGE", c.cmd("ZRANGE", "z", "0", "-1"), lambda r: isinstance(r, list) and "b" in r and "c" in r)
        ck.check("ZPOPMIN", c.cmd("ZPOPMIN", "z"), lambda r: isinstance(r, list) and r[0] == "b")  # a was bumped to 5
        ck.check("ZREM", c.cmd("ZREM", "z", "c"), lambda r: r == "1")

        # --- wrong type ---
        c.cmd("SET", "str", "v")
        ck.check("ZADD wrongtype", c.cmd("ZADD", "str", "1", "m"), lambda r: isinstance(r, str) and r[0:1] != "")

        # --- errors ---
        ck.check("arity error", c.cmd("SET", "onlykey"), lambda r: isinstance(r, str) and r.startswith("ERR"))
        ck.check("unknown cmd", c.cmd("BOGUS", "x"), lambda r: isinstance(r, str) and r.startswith("ERR"))

        # --- FLUSHDB ---
        c.cmd("SET", "ff", "v")
        ck.check("FLUSHDB", c.cmd("FLUSHDB"), lambda r: r == "OK")
        ck.check("gone after flush", c.cmd("GET", "ff"), lambda r: r is None)

        # --- pipelining: many commands in one send, responses batch in order ---
        pipe = b"".join(enc("SET", f"p{i}", str(i)) for i in range(50))
        pipe += b"".join(enc("GET", f"p{i}") for i in range(50))
        c.send_raw(pipe)
        oks = [c.read() for _ in range(50)]
        vals = [c.read() for _ in range(50)]
        ck.check("pipeline SET oks", oks, lambda r: all(x == "OK" for x in r))
        ck.check("pipeline GET order", vals, lambda r: r == [str(i) for i in range(50)])

        # --- big value forces client input buffer growth (> 4096) ---
        big = "x" * 100000
        ck.check("SET big", c.cmd("SET", "big", big), lambda r: r == "OK")
        ck.check("GET big round-trips (len 100000)", c.cmd("GET", "big") == big, lambda r: r is True)

        # --- multiple concurrent clients ---
        clients = [Client(PORT) for _ in range(10)]
        for i, cl in enumerate(clients):
            cl.cmd("SET", f"c{i}", f"val{i}")
        allok = all(clients[i].cmd("GET", f"c{i}") == f"val{i}" for i in range(10))
        ck.check("multi-client isolation", allok, lambda r: r is True)
        for cl in clients:
            cl.close()

        # --- WAIT with no replicas returns immediately ---
        ck.check("WAIT 0 100 -> 0", c.cmd("WAIT", "0", "100"), lambda r: r == "0")
        # WAIT 1 with no replicas parks, then resolves to 0 at the deadline
        ck.check("WAIT 1 300 parks then -> 0", c.cmd("WAIT", "1", "300"), lambda r: r == "0")

        # --- split frame across two sends: server must buffer partial input ---
        frame = enc("SET", "split", "ok")
        c.send_raw(frame[:6]); c.send_raw(frame[6:])
        ck.check("partial-frame SET", c.read(), lambda r: r == "OK")
        ck.check("partial-frame GET", c.cmd("GET", "split"), lambda r: r == "ok")

        # --- malformed RESP: server replies an error (or drops the client) ---
        bad = Client(PORT)
        try:
            bad.send_raw(b"@notresp\r\n")
            r = bad.read()
            ck.check("protocol error handled", r, lambda x: isinstance(x, str) and x.startswith("ERR"))
        except (ConnectionError, OSError):
            ck.check("protocol error handled (closed)", True, lambda x: x is True)
        finally:
            bad.close()

        # --- WAIT parked, then client disconnects: server cancels the pending wait ---
        waiter = Client(PORT)
        waiter.send_raw(enc("WAIT", "1", "5000"))   # parks (no replicas)
        import time as _t; _t.sleep(0.2)
        waiter.close()                               # disconnect while parked
        ck.check("server survives parked-WAIT disconnect", c.cmd("PING"), lambda r: r == "PONG")

        # --- output backpressure: large reply the client drains slowly ---
        # forces flush_client_output to hit EAGAIN, arm the writable event, and
        # grow the client out_buffer (append_client_output expand path).
        c.cmd("SET", "bp", "y" * 100000)             # 100KB value
        bp = Client(PORT)
        bp.send_raw(b"".join(enc("GET", "bp") for _ in range(100)))  # ~10MB of replies
        _t.sleep(0.3)                                # let the server buffer + arm writable
        got = 0
        for _ in range(100):
            v = bp.read()
            if v is not None:
                got += 1
        ck.check("backpressure: all 100 large replies drained", got, lambda r: r == 100)
        bp.close()

        # --- client disconnect mid-session, server keeps serving others ---
        victim = Client(PORT)
        victim.cmd("SET", "vk", "vv")
        victim.close()  # abrupt close
        ck.check("server alive after disconnect", c.cmd("GET", "vk"), lambda r: r == "vv")

        c.close()
    finally:
        stop_server(p)
    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
