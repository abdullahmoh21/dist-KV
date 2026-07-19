"""AOF durability + the Phase-2 absolute-time trap: EXPIRE/PEXPIRE must persist
as absolute PEXPIREAT (never relative), deadlines survive restart, and the
active-expiry sweep propagates a DEL into the AOF."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, Checker, run_dir_for

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7040


def main():
    rd = run_dir_for("durability")
    # start clean so the AOF assertions are deterministic
    aof = os.path.join(rd, "appendonly.aof")
    os.makedirs(rd, exist_ok=True)
    if os.path.exists(aof):
        os.remove(aof)
    ck = Checker("durability")

    p = start_server(BIN, rd, PORT)
    c = Client(PORT)
    ck.check("SET a", c.cmd("SET", "a", "v"), lambda r: r == "OK")
    ck.check("PEXPIRE a 100000", c.cmd("PEXPIRE", "a", "100000"), lambda r: r == "1")
    pttl_before = int(c.cmd("PTTL", "a"))
    ck.check("PTTL a ~100000", pttl_before, lambda r: 95000 < r <= 100000)
    c.cmd("SET", "b", "v"); c.cmd("EXPIRE", "b", "100")     # relative -> must become PEXPIREAT
    c.cmd("SET", "dead", "v"); c.cmd("PEXPIRE", "dead", "200")
    c.close()
    time.sleep(4.0)  # let the active-expiry sweep reap 'dead' AND the 1s AOF flush timer sync it.
                     # generous margin: under the full suite this server's event-loop tick (which
                     # drives both the sweep and the flush) competes for CPU with ~8 other
                     # coverage-instrumented servers, so a tight window flakes.
    stop_server(p)

    data = open(aof, "rb").read()
    ck.check("AOF has PEXPIREAT (absolute)", data.count(b"PEXPIREAT\r\n"), lambda n: n >= 2)
    ck.check("AOF has NO relative PEXPIRE", data.count(b"\nPEXPIRE\r\n"), lambda n: n == 0)
    ck.check("AOF has NO relative EXPIRE", data.count(b"\nEXPIRE\r\n"), lambda n: n == 0)
    ck.check("active-expiry propagated DEL dead", b"DEL\r\n$4\r\ndead\r\n" in data, lambda r: r is True)

    # restart: absolute deadline survives replay
    p = start_server(BIN, rd, PORT)
    c = Client(PORT)
    ck.check("PTTL a survives restart", int(c.cmd("PTTL", "a")), lambda r: 90000 < r <= 100000)
    ck.check("b alive after restart", c.cmd("GET", "b"), lambda r: r == "v")
    ck.check("dead gone after restart", c.cmd("GET", "dead"), lambda r: r is None)
    c.close()
    stop_server(p)
    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
