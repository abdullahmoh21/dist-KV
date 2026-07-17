#!/usr/bin/env bash
#
# Full dist-KV test suite + coverage gate.
#
#   1. Runs the C unit binary (storage / parser / engine layers, in-process).
#   2. Runs the Python integration suites against the instrumented server
#      (server.c, event loop, AOF, compaction fork, replication).
#   3. Merges every coverage profile (unit + all server processes, incl. forked
#      compaction children and replica processes via the %p pid pattern).
#   4. Prints a per-file line-coverage table and fails if the project total is
#      below THRESHOLD or if any test failed.
#
# Usage: bash tests/run_tests.sh   (normally invoked via `make test`)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

COVDIR="build/cov"
THRESHOLD="${COV_THRESHOLD:-80}"
SERVER_BIN="$COVDIR/server_cov"
UNIT_BIN="$COVDIR/unit_tests"
PY="${PYTHON:-python3}"

RUNDIR="$COVDIR/run"
rm -rf "$RUNDIR"; mkdir -p "$RUNDIR"
export DKV_TEST_RUNDIR="$RUNDIR"

# fresh profiles
rm -f "$COVDIR"/*.profraw "$COVDIR"/*.profdata

# Absolute — servers run with cwd set to their per-test run dir, so a relative
# profile pattern would scatter .profraw files across those dirs.
PROFDIR="$ROOT/$COVDIR/profraw"
rm -rf "$PROFDIR"; mkdir -p "$PROFDIR"

fail=0

echo "==================== UNIT ===================="
# %c (continuous mode): the aof-compaction suite fork()s a child that _exit()s
# from inside aof_compact_to_file, so its counters must be mmap'd live to survive.
LLVM_PROFILE_FILE="$PROFDIR/unit-%p%c.profraw" "$UNIT_BIN" || fail=1

echo ""
echo "================ INTEGRATION ================="
# Each server process (and any fork/replica) writes its own profraw via %p.
# %c enables continuous mode: counters are mmap'd live to the file, so the
# profile survives the SIGINT the harness uses to stop the server (which
# otherwise skips the atexit profile writer and leaves a 0-byte file).
export LLVM_PROFILE_FILE="$PROFDIR/server-%p%c.profraw"

PORT=7010
for t in test_commands test_expiry test_set_options test_bzpopmin test_queue test_durability test_compaction test_aof_load test_replication; do
    echo "---- $t ----"
    "$PY" "tests/integration/$t.py" "$SERVER_BIN" "$PORT" || fail=1
    PORT=$((PORT + 10))
done
unset LLVM_PROFILE_FILE

echo "==================== COVERAGE ===================="
PROFS=("$PROFDIR"/*.profraw)
if [ ! -e "${PROFS[0]}" ]; then
    echo "no coverage profiles produced" >&2
    exit 1
fi
xcrun llvm-profdata merge -sparse "${PROFS[@]}" -o "$COVDIR/merged.profdata"

# Coverage is measured over the implementation sources (kv/src/*.c). Debug info
# records absolute paths, so filter by substring: drop project headers, the test
# code itself, system/SDK headers, and the non-compiled epoll backend.
IGNORE='(kv/include/|/tests/|/usr/|/Library/|/Applications/|event_loop_epoll)'

# Human-readable per-file table (project .c files only).
xcrun llvm-cov report \
    "$SERVER_BIN" -object "$UNIT_BIN" \
    -instr-profile="$COVDIR/merged.profdata" \
    -ignore-filename-regex="$IGNORE" 2>/dev/null \
    | grep -E "Filename|kv/src/.*\.c|^TOTAL" | grep -v '\.h'

# Machine-readable summary for the gate.
xcrun llvm-cov export -summary-only \
    "$SERVER_BIN" -object "$UNIT_BIN" \
    -instr-profile="$COVDIR/merged.profdata" \
    -ignore-filename-regex="$IGNORE" 2>/dev/null > "$COVDIR/summary.json"

PCT=$("$PY" - "$COVDIR/summary.json" <<'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
tot = data["data"][0]["totals"]["lines"]
print("%.2f" % tot["percent"])
PYEOF
)

echo ""
echo "Project line coverage: ${PCT}%  (threshold ${THRESHOLD}%)"

# gate
awk -v p="$PCT" -v t="$THRESHOLD" 'BEGIN { exit (p+0 >= t+0) ? 0 : 1 }' || {
    echo "FAIL: coverage ${PCT}% is below ${THRESHOLD}%"
    fail=1
}

if [ "$fail" -ne 0 ]; then
    echo ""
    echo "RESULT: FAILURES (tests failing and/or coverage below threshold)"
    exit 1
fi
echo ""
echo "RESULT: ALL GREEN — tests pass and coverage >= ${THRESHOLD}%"
