"""AOF replay paths: a valid AOF with mixed command types must reconstruct
state, and corrupt/truncated AOFs must be rejected (the server exits non-zero).
Drives the aof_load.c parse + dispatch loop and its error branches."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Client, start_server, stop_server, launch_and_wait, Checker, run_dir_for

BIN = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7060


def write_aof(rd, data):
    os.makedirs(rd, exist_ok=True)
    with open(os.path.join(rd, "appendonly.aof"), "wb") as f:
        f.write(data)


def frame(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def main():
    ck = Checker("aof_load")

    # 1) valid AOF with mixed command types replays and serves
    rd = run_dir_for("aofload_valid")
    aof = (frame("SET", "a", "1")
           + frame("INCR", "a")
           + frame("ZADD", "z", "5", "m")
           + frame("SET", "d", "x")
           + frame("DEL", "d")
           + frame("PEXPIREAT", "a", str(9999999999999)))
    write_aof(rd, aof)
    p = start_server(BIN, rd, PORT)
    c = Client(PORT)
    ck.check("replayed SET+INCR", c.cmd("GET", "a"), lambda r: r == "2")
    ck.check("replayed ZADD", c.cmd("ZSCORE", "z", "m"), lambda r: r == "5")
    ck.check("replayed DEL removed key", c.cmd("GET", "d"), lambda r: r is None)
    ck.check("replayed PEXPIREAT set a TTL", c.cmd("PTTL", "a"), lambda r: int(r) > 0)
    c.close()
    stop_server(p)

    # 2) truncated frame -> EOF with unparsed bytes -> AOF_PARSE_ERR -> exit 1
    rd = run_dir_for("aofload_truncated")
    write_aof(rd, frame("SET", "k", "v") + b"*3\r\n$3\r\nSET\r\n$3\r\nfo")  # dangling
    rc = launch_and_wait(BIN, rd)
    ck.check("truncated AOF -> non-zero exit", rc, lambda r: r != 0)

    # 3) invalid type byte -> ERR_INVALID_TYPE -> exit 1
    rd = run_dir_for("aofload_badtype")
    write_aof(rd, b"xNOTVALIDRESP\r\n")
    rc = launch_and_wait(BIN, rd)
    ck.check("garbage AOF -> non-zero exit", rc, lambda r: r != 0)

    return ck.done()


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
