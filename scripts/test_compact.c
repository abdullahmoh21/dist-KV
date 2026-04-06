/*
 * test_compact.c — standalone compaction test driver.
 *
 * Loads appendonly.aof into a RedisStore (same path aof_load uses),
 * then forks and calls aof_compact() directly in the child.
 * The parent waits and reports exit status / signal.
 *
 * Build + run via scripts/run_compact_test.sh from the repo root.
 */

#include <aof/aof.h>
#include <store/redis_store.h>
#include <store/hashmap.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>

int main(void) {
    // ── 1. Create store ──────────────────────────────────────────────────────
    RedisStore store;
    fprintf(stderr, "[test] creating store...\n");
    if (create_store(&store) != RS_OK) {
        fprintf(stderr, "[test] FAIL: create_store\n");
        return 1;
    }

    // ── 2. Load AOF ──────────────────────────────────────────────────────────
    struct stat aof_st;
    if (stat("appendonly.aof", &aof_st) != 0) {
        fprintf(stderr, "[test] FAIL: appendonly.aof not found in cwd (%s)."
                        " Run this script from the repo root.\n", strerror(2));
        return 1;
    }
    fprintf(stderr, "[test] loading appendonly.aof (%lld bytes)...\n",
            (long long)aof_st.st_size);

    enum AOF_RESULT load_res = aof_load(&store);
    if (load_res != AOF_OK) {
        fprintf(stderr, "[test] FAIL: aof_load returned %d\n", load_res);
        return 1;
    }
    fprintf(stderr, "[test] store loaded — %zu keys in dict\n",
            store.dict->item_count);

    // ── 3. Fork → child runs aof_compact, parent waits ──────────────────────
    fprintf(stderr, "[test] forking compaction child...\n");
    fflush(stderr);

    pid_t pid = fork();
    if (pid < 0) {
        perror("[test] FAIL: fork");
        return 1;
    }

    if (pid == 0) {
        // Child: aof_compact calls _exit(0) on success, _exit(1) on failure.
        // No AOF thread, no mutex — pure compaction logic.
        aof_compact(&store);
        _exit(1); // unreachable, but satisfies the compiler
    }

    // ── 4. Parent: wait and decode child exit ─────────────────────────────────
    int status = 0;
    pid_t waited = waitpid(pid, &status, 0);
    if (waited < 0) {
        perror("[test] FAIL: waitpid");
        return 1;
    }

    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        if (code == 0) {
            struct stat cs;
            long long compacted_size = 0;
            if (stat("compacted.aof", &cs) == 0) compacted_size = cs.st_size;
            fprintf(stderr, "[test] PASS: child exited 0 — compacted.aof is %lld bytes"
                            " (%.1f%% of original)\n",
                    compacted_size,
                    aof_st.st_size > 0
                        ? 100.0 * (double)compacted_size / (double)aof_st.st_size
                        : 0.0);
        } else {
            fprintf(stderr, "[test] FAIL: child exited %d\n", code);
        }
        return code;
    }

    if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        fprintf(stderr, "[test] FAIL: child killed by signal %d (%s)\n",
                sig, strsignal(sig));
#ifdef WCOREDUMP
        if (WCOREDUMP(status))
            fprintf(stderr, "[test]       core dumped\n");
#endif
        return 1;
    }

    fprintf(stderr, "[test] FAIL: child in unknown state (status=%d)\n", status);
    return 1;
}
