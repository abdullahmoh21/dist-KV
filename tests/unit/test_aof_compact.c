#define _DARWIN_C_SOURCE   /* expose memmem under -std=c11 */
#include "test_framework.h"
#include "store/redis_store.h"
#include "store/object.h"
#include "aof/aof.h"
#include "utils/time.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>

/*
 * aof_compact_to_file() walks the store and serializes it as RESP SET/ZADD/
 * PEXPIREAT commands — the heart of fork-based compaction. It ends in _exit()
 * because it's designed to run in the compaction child, so we drive it exactly
 * that way: fork, let the child serialize to a file, then verify the bytes in
 * the parent. (Under continuous coverage mode the child's counters survive the
 * _exit, so this still measures aof_compact.c + aof_resp_encode.c.)
 */

static BulkString bs(const char *s) {
    BulkString b; b.data = (char *)s; b.len = strlen(s); return b;
}

static char *slurp(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)n + 1);
    size_t got = fread(buf, 1, (size_t)n, f);
    fclose(f);
    buf[got] = '\0';
    *out_len = got;
    return buf;
}

static int contains(const char *hay, size_t hlen, const char *needle) {
    return memmem(hay, hlen, needle, strlen(needle)) != NULL;
}

int run_aof_compact_tests(void) {
    TF_SUITE_BEGIN();
    RedisStore store;
    create_store(&store);

    /* plain KV */
    BulkString k = bs("ck"), v = bs("cv");
    rs_set(&store, &k, &v);

    /* KV carrying a live TTL -> must re-emit PEXPIREAT */
    BulkString kt = bs("ckt"), vt = bs("cvt");
    rs_set(&store, &kt, &vt);
    rs_set_expire(&store, &kt, wallclock_ms() + 100000);

    /* already-expired KV -> must be skipped by the walk */
    BulkString ke = bs("cke_expired"), ve = bs("x");
    rs_set(&store, &ke, &ve);
    rs_set_expire(&store, &ke, 1);   /* deadline in the past */

    /* Enough KV bulk (> COMPACT_BUF_SIZE = 4MB) that the walk has to flush the
     * mmap'd buffer to disk mid-pass -> covers _buffer_ensure_space's flush. */
    static char bigval[1500];
    memset(bigval, 'V', sizeof(bigval) - 1); bigval[sizeof(bigval) - 1] = '\0';
    char kbuf[24];
    for (int i = 0; i < 3200; i++) {
        snprintf(kbuf, sizeof(kbuf), "bulk%d", i);
        BulkString bk = bs(kbuf), bv = bs(bigval);
        rs_set(&store, &bk, &bv);
    }

    /* big ZSET (> ZSET_BATCH_SIZE=1000) to exercise the batch-flush branch */
    BulkString zb = bs("czb");
    char mb[24];
    for (int i = 0; i < 1500; i++) {
        snprintf(mb, sizeof(mb), "mem%d", i);
        BulkString mm = bs(mb);
        rs_zadd(&store, &zb, &mm, (double)i);
    }
    /* give the ZSET a TTL too -> covers PEXPIREAT-after-ZADD */
    rs_set_expire(&store, &zb, wallclock_ms() + 100000);

    const char *path = "/tmp/dkv_unit_compact.aof";
    unlink(path);

    pid_t pid = fork();
    if (pid == 0) {
        /* Reopen the coverage profile under the child's own pid so continuous
         * mode maps this child's counters to its own file (the child inherits
         * the parent's mapping otherwise and its counts would be lost at
         * _exit). Weak symbol: a no-op when built without instrumentation. */
        extern void __llvm_profile_initialize_file(void) __attribute__((weak));
        if (__llvm_profile_initialize_file) __llvm_profile_initialize_file();
        /* child: serialize the store, then _exit() from inside the function */
        aof_compact_to_file(&store, path);
        _exit(2);   /* unreachable: aof_compact_to_file always _exit()s */
    }

    int status = 0;
    waitpid(pid, &status, 0);

    TEST("compaction child exited cleanly") {
        CHECK(WIFEXITED(status));
        CHECK_EQ_INT(WEXITSTATUS(status), 0);
    }

    size_t len = 0;
    char *data = slurp(path, &len);
    TEST("compacted file has the expected frames") {
        CHECK_NOT_NULL(data);
        if (data) {
            CHECK(contains(data, len, "SET"));
            CHECK(contains(data, len, "ZADD"));
            CHECK(contains(data, len, "PEXPIREAT"));
            CHECK(contains(data, len, "czb"));       /* the big zset key */
            CHECK(contains(data, len, "mem1200"));   /* a member past the batch boundary */
            CHECK(!contains(data, len, "cke_expired"));  /* expired key skipped */
        }
    }
    free(data);

    return _tf_fails;
}
