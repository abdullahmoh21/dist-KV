#!/usr/bin/env python3
"""
bloat_aof.py — sends repeated SET commands for the same keys to bloat the AOF
without growing the in-memory store proportionally.

Usage:
    python3 scripts/bloat_aof.py [--host HOST] [--port PORT]
                                  [--keys NUM_KEYS] [--rounds ROUNDS]
                                  [--batch BATCH_SIZE]

Effect on disk:
    AOF size  ≈ NUM_KEYS * ROUNDS * ~50 bytes per SET command
    Store size ≈ NUM_KEYS keys  (duplicates overwrite in memory)

This creates a high compaction ratio: store is small but AOF is huge.
AOF compaction triggers when file_size > last_compaction_size * 2.
"""

import socket
import time
import argparse
import sys

# ---------------------------------------------------------------------------
# RESP helpers
# ---------------------------------------------------------------------------

def encode_set(key: str, value: str) -> bytes:
    k = key.encode()
    v = value.encode()
    return (
        f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n".encode()
        + k + b"\r\n"
        + f"${len(v)}\r\n".encode()
        + v + b"\r\n"
    )


def drain_responses(sock: socket.socket, count: int) -> int:
    """Read and count +OK / error responses from the socket."""
    received = 0
    buf = b""
    while received < count:
        try:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
            received += buf.count(b"\r\n")
            # Each RESP simple/bulk reply ends with \r\n; +OK\r\n = 1 \r\n per reply
            # Rough count is fine here — we just need to drain the socket
        except BlockingIOError:
            break
    return received


# ---------------------------------------------------------------------------
# Core sender
# ---------------------------------------------------------------------------

def run(host: str, port: int, num_keys: int, rounds: int, batch_size: int) -> None:
    keys = [f"dupkey:{i:06d}" for i in range(num_keys)]

    total_commands = num_keys * rounds
    approx_aof_bytes = total_commands * 55  # rough bytes per SET in RESP
    print(f"[config] {num_keys} unique keys × {rounds} rounds = {total_commands:,} SET commands")
    print(f"[config] estimated AOF bloat: ~{approx_aof_bytes / 1024:.1f} KB")
    print(f"[config] target: {host}:{port}  batch size: {batch_size}")
    print()

    start = time.time()
    grand_total = 0

    for rnd in range(1, rounds + 1):
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError as e:
            print(f"[error] cannot connect to {host}:{port}: {e}", file=sys.stderr)
            sys.exit(1)

        buf = bytearray()
        commands_in_buf = 0
        commands_this_round = 0

        for key in keys:
            val = f"round{rnd}:val"
            buf += encode_set(key, val)
            commands_in_buf += 1

            if commands_in_buf >= batch_size:
                sock.sendall(buf)
                drain_responses(sock, commands_in_buf)
                grand_total += commands_in_buf
                commands_this_round += commands_in_buf
                buf = bytearray()
                commands_in_buf = 0

        # flush remainder
        if commands_in_buf:
            sock.sendall(buf)
            drain_responses(sock, commands_in_buf)
            grand_total += commands_in_buf
            commands_this_round += commands_in_buf

        sock.close()

        elapsed = time.time() - start
        rps = grand_total / elapsed if elapsed > 0 else 0
        pct = 100 * rnd / rounds
        print(
            f"[round {rnd:>3}/{rounds}]  "
            f"sent this round: {commands_this_round:>6,}  "
            f"total: {grand_total:>8,}  "
            f"rps: {rps:>8,.0f}  "
            f"({pct:.0f}%)"
        )

    elapsed = time.time() - start
    print()
    print(f"[done] {grand_total:,} commands in {elapsed:.2f}s  ({grand_total/elapsed:,.0f} rps avg)")
    print(f"[done] AOF should be ~{grand_total * 55 / 1024:.0f} KB with only {num_keys} live keys")
    print(f"[done] compaction ratio: ~{rounds}:1")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host",   default="127.0.0.1")
    p.add_argument("--port",   default=6379, type=int)
    p.add_argument("--keys",   default=500,  type=int, help="number of unique keys (default 500)")
    p.add_argument("--rounds", default=50,   type=int, help="times each key is overwritten (default 50)")
    p.add_argument("--batch",  default=500,  type=int, help="commands per TCP flush (default 500)")
    args = p.parse_args()

    run(args.host, args.port, args.keys, args.rounds, args.batch)
