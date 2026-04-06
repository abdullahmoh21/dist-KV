#!/usr/bin/env bash
# run_compact_test.sh — full AOF compaction test.
#
# 1. Builds the server and the test binary.
# 2. Starts the server, bloats its AOF with bloat_aof.py, then stops it.
# 3. Forks the compaction child directly (no server) and checks the result.
#
# Usage (from repo root):   bash scripts/run_compact_test.sh
#                           make test-compact

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CC="${CC:-gcc}"
CFLAGS="-Wall -Wextra -std=c11 -g -O0 -Ikv/include"
OUT="build/test_compact"
SERVER_BIN="./server_build"
SERVER_PID=""

# ── Cleanup: always stop the background server on exit ───────────────────────
cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "[setup] stopping server (pid=$SERVER_PID)..."
        kill "$SERVER_PID"
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Platform flags ────────────────────────────────────────────────────────────
case "$(uname -s)" in
    Darwin) CFLAGS="$CFLAGS -DPLATFORM_MACOS" ;;
    Linux)  CFLAGS="$CFLAGS -DPLATFORM_LINUX" ;;
    *)      echo "Unsupported OS: $(uname -s)"; exit 1 ;;
esac

# Sources: same as the server build minus server.c and the event loop.
SRCS=(
    scripts/test_compact.c
    kv/src/engine/execution_engine.c
    kv/src/parser/resp_parser.c
    kv/src/store/buffer.c
    kv/src/store/hashmap.c
    kv/src/store/redis_store.c
    kv/src/store/skip_list.c
    kv/src/utils/time.c
    kv/src/utils/fast_format.c
    kv/src/utils/fast_parse.c
    kv/src/aof/aof.c
    kv/src/aof/aof_manager.c
    kv/src/aof/aof_load.c
    kv/src/aof/aof_compact.c
    kv/src/aof/aof_resp_encode.c
)

# ── 1. Build server + test binary ─────────────────────────────────────────────
echo "=== Building ==="
make all --no-print-directory

mkdir -p build
# shellcheck disable=SC2086
$CC $CFLAGS "${SRCS[@]}" -o "$OUT" -lpthread
echo "Built: $OUT"

# ── 2. Start server ───────────────────────────────────────────────────────────
echo ""
echo "=== Starting server ==="

# Kill any stale process on 6379
if lsof -ti:6379 >/dev/null 2>&1; then
    echo "[setup] killing existing process on port 6379..."
    kill "$(lsof -ti:6379)" 2>/dev/null || true
    sleep 0.3
fi

# Fresh slate
rm -f appendonly.aof compacted.aof tmp.aof

$SERVER_BIN > /tmp/dist-kv-server.log 2>&1 &
SERVER_PID=$!
echo "[setup] server pid=$SERVER_PID"

# Wait until the server accepts connections (up to 2s)
for i in $(seq 1 20); do
    if nc -z 127.0.0.1 6379 2>/dev/null; then
        echo "[setup] server ready"
        break
    fi
    sleep 0.1
    if [ "$i" -eq 20 ]; then
        echo "[setup] ERROR: server did not start within 2s"
        exit 1
    fi
done

# ── 3. Bloat the AOF ──────────────────────────────────────────────────────────
echo ""
echo "=== Bloating AOF ==="
python3 scripts/bloat_aof.py

# Give the AOF background flush thread one full cycle (~1s) to drain
echo "[setup] waiting for AOF flush..."
sleep 1.5

# ── 4. Stop server ────────────────────────────────────────────────────────────
echo ""
echo "=== Stopping server ==="
if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID"
    wait "$SERVER_PID" 2>/dev/null || true
else
    echo "[setup] server already exited — check /tmp/dist-kv-server.log"
    echo "--- server log ---"
    tail -20 /tmp/dist-kv-server.log || true
    echo "-----------------"
fi
SERVER_PID=""  # prevent double-kill in trap

# ── 5. Run compaction test ────────────────────────────────────────────────────
echo ""
echo "=== Running compaction test ==="
rm -f compacted.aof

"$OUT"
STATUS=$?

echo ""
if [ $STATUS -eq 0 ]; then
    echo "=== RESULT: compaction PASSED ==="
    ls -lh compacted.aof 2>/dev/null || echo "(compacted.aof not found — unexpected)"
else
    echo "=== RESULT: compaction FAILED (exit $STATUS) ==="
    echo "    → Signal 10 (SIGBUS) = fault inside aof_compact(), not fork coordination."
fi

exit $STATUS
