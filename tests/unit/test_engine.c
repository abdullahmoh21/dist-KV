#include "test_framework.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "store/object.h"
#include "utils/time.h"
#include <stdio.h>
#include <stdint.h>

/*
 * Engine-level tests: drive dispatch_command + every handler + reply.c in
 * process. A capturing reply writer collects the RESP bytes each command
 * emits so we can assert on the wire response without a socket.
 */

#define CAP_SZ (128 * 1024)
static char  g_cap[CAP_SZ];
static size_t g_cap_used;

static ExecuteResult cap_writer(int fd, const char *data, size_t len, void *ctx) {
    (void)fd; (void)ctx;
    if (g_cap_used + len < CAP_SZ) {
        memcpy(g_cap + g_cap_used, data, len);
        g_cap_used += len;
    }
    return EE_OK;
}
static void cap_reset(void) { g_cap_used = 0; g_cap[0] = '\0'; }
static const char *cap(void) { g_cap[g_cap_used] = '\0'; return g_cap; }
static int cap_has(const char *needle) { return strstr(cap(), needle) != NULL; }

/* Build a RESP array frame from argv and parse it into *out. Returns bytes consumed. */
static char g_frame[8192];
static ssize_t build(RedisCommand *out, int argc, const char **argv) {
    int off = snprintf(g_frame, sizeof(g_frame), "*%d\r\n", argc);
    for (int i = 0; i < argc; i++) {
        size_t l = strlen(argv[i]);
        off += snprintf(g_frame + off, sizeof(g_frame) - off, "$%zu\r\n", l);
        memcpy(g_frame + off, argv[i], l); off += (int)l;
        g_frame[off++] = '\r'; g_frame[off++] = '\n';
    }
    return parse_array_command(g_frame, (size_t)off, out);
}

/* Dispatch one command (given as argv) and return the ExecuteResult. */
static ExecuteResult run(RedisStore *store, int argc, const char **argv) {
    RedisCommand cmd;
    cap_reset();
    build(&cmd, argc, argv);
    ExecuteResult r = dispatch_command(1, &cmd, store);
    free_command(&cmd);
    return r;
}

#define ARGV(...) ((const char *[]){__VA_ARGS__})
#define NARGS(...) ((int)(sizeof((const char *[]){__VA_ARGS__}) / sizeof(char *)))
#define RUN(store, ...) run((store), NARGS(__VA_ARGS__), ARGV(__VA_ARGS__))

int run_engine_tests(void) {
    TF_SUITE_BEGIN();
    ee_set_reply_writer(cap_writer, NULL);
    RedisStore store;
    create_store(&store);

    TEST("PING -> +PONG") {
        ExecuteResult r = RUN(&store, "PING");
        CHECK_EQ_INT(r, EE_OK);
        CHECK(cap_has("+PONG"));
    }

    TEST("SET -> +OK, GET round-trips") {
        CHECK_EQ_INT(RUN(&store, "SET", "k", "hello"), EE_WRITE_OK);
        CHECK(cap_has("+OK"));
        RUN(&store, "GET", "k");
        CHECK(cap_has("$5\r\nhello\r\n"));
    }

    TEST("GET missing -> null bulk") {
        RUN(&store, "GET", "absent");
        CHECK(cap_has("$-1"));
    }

    TEST("DEL present then absent") {
        RUN(&store, "SET", "d", "v");
        RUN(&store, "DEL", "d");
        CHECK(cap_has(":1"));
        RUN(&store, "DEL", "d");
        CHECK(cap_has(":0"));
    }

    TEST("INCR / DECR / INCRBY / DECRBY") {
        RUN(&store, "INCR", "ctr");   CHECK(cap_has(":1"));
        RUN(&store, "INCR", "ctr");   CHECK(cap_has(":2"));
        RUN(&store, "INCRBY", "ctr", "10"); CHECK(cap_has(":12"));
        RUN(&store, "DECR", "ctr");   CHECK(cap_has(":11"));
        RUN(&store, "DECRBY", "ctr", "5"); CHECK(cap_has(":6"));
    }

    TEST("INCR on non-integer value -> error") {
        RUN(&store, "SET", "s", "abc");
        ExecuteResult r = RUN(&store, "INCR", "s");
        CHECK(cap()[0] == '-');      /* -ERR not an integer */
        (void)r;
    }

    TEST("ZADD / ZSCORE / update") {
        RUN(&store, "ZADD", "z", "1", "a"); CHECK(cap_has(":1"));
        RUN(&store, "ZADD", "z", "2", "a"); CHECK(cap_has(":0"));  /* update, 0 added */
        RUN(&store, "ZSCORE", "z", "a");
        CHECK(cap_has("2"));
        RUN(&store, "ZSCORE", "z", "missing");
        CHECK(cap_has("$-1"));
    }

    TEST("ZADD multi-member + ZRANGE + ZPOPMIN + ZREM") {
        RUN(&store, "ZADD", "zq", "3", "c", "1", "a", "2", "b");
        RUN(&store, "ZRANGE", "zq", "0", "-1");
        CHECK(cap_has("a"));  CHECK(cap_has("b"));  CHECK(cap_has("c"));
        RUN(&store, "ZPOPMIN", "zq");
        CHECK(cap_has("a"));   /* lowest score popped */
        RUN(&store, "ZREM", "zq", "b");
        CHECK(cap_has(":1"));
    }

    TEST("wrong-type: ZADD on a KV key errors") {
        RUN(&store, "SET", "str", "v");
        RUN(&store, "ZADD", "str", "1", "m");
        CHECK(cap()[0] == '-');
    }

    TEST("EXPIRE + TTL + PTTL + PERSIST") {
        RUN(&store, "SET", "e", "v");
        RUN(&store, "EXPIRE", "e", "100"); CHECK(cap_has(":1"));
        RUN(&store, "TTL", "e");
        CHECK(cap_has(":100") || cap_has(":99"));
        RUN(&store, "PTTL", "e");
        CHECK(cap_has(":9") || cap_has(":10"));  /* ~100000ms */
        RUN(&store, "PERSIST", "e"); CHECK(cap_has(":1"));
        RUN(&store, "TTL", "e"); CHECK(cap_has(":-1"));
    }

    TEST("PEXPIRE + PEXPIREAT") {
        RUN(&store, "SET", "p", "v");
        RUN(&store, "PEXPIRE", "p", "50000"); CHECK(cap_has(":1"));
        char abs[32];
        snprintf(abs, sizeof(abs), "%llu", (unsigned long long)(wallclock_ms() + 60000));
        RUN(&store, "PEXPIREAT", "p", abs); CHECK(cap_has(":1"));
        RUN(&store, "PTTL", "p");
        CHECK(cap_has(":5") || cap_has(":6"));
    }

    TEST("TTL semantics: missing -> -2, no-ttl -> -1") {
        RUN(&store, "TTL", "nokey"); CHECK(cap_has(":-2"));
        RUN(&store, "SET", "nt", "v");
        RUN(&store, "TTL", "nt"); CHECK(cap_has(":-1"));
    }

    TEST("EXPIRE on missing key -> 0") {
        RUN(&store, "EXPIRE", "ghostkey", "10");
        CHECK(cap_has(":0"));
    }

    TEST("arity error is reported") {
        ExecuteResult r = RUN(&store, "SET", "onlykey");  /* SET needs 3 args */
        CHECK_EQ_INT(r, EE_ERR_ARITY);
        CHECK(cap()[0] == '-');
    }

    TEST("unknown command reported") {
        ExecuteResult r = RUN(&store, "NOTACMD", "x");
        CHECK_EQ_INT(r, EE_COMMAND_NOT_FOUND);
        CHECK(cap()[0] == '-');
    }

    TEST("FLUSHDB clears the store") {
        RUN(&store, "SET", "ff", "v");
        RUN(&store, "FLUSHDB");
        RUN(&store, "GET", "ff");
        CHECK(cap_has("$-1"));
    }

    TEST("COMMAND introspection (array reply)") {
        RUN(&store, "COMMAND");
        CHECK(cap_has("get"));
        CHECK(cap_has("set"));
        RUN(&store, "COMMAND", "DOCS");
        CHECK(cap_has("*0") || g_cap_used > 0);
    }

    /* ---- ZSET error / option branches ---- */

    TEST("ZADD odd arity (dangling score) errors") {
        RUN(&store, "ZADD", "ze", "1", "a", "2");   /* 5 args -> odd -> handler error */
        CHECK(cap()[0] == '-');
    }

    TEST("ZADD non-numeric score errors") {
        RUN(&store, "ZADD", "ze2", "notanum", "m");
        CHECK(cap()[0] == '-');
    }

    TEST("ZSCORE wrong-type / missing key") {
        RUN(&store, "SET", "kvk", "v");
        RUN(&store, "ZSCORE", "kvk", "m");
        CHECK(cap()[0] == '-');                     /* WRONGTYPE */
        RUN(&store, "ZSCORE", "nozset", "m");
        CHECK(cap_has("$-1"));                       /* missing key -> nil */
    }

    TEST("ZREM missing key -> 0, wrong-type -> error") {
        RUN(&store, "ZREM", "nozset", "m");
        CHECK(cap_has(":0"));
        RUN(&store, "ZREM", "kvk", "m");
        CHECK(cap()[0] == '-');
    }

    TEST("ZRANGE index + BYSCORE + WITHSCORES + negatives") {
        RUN(&store, "ZADD", "zr", "1", "a", "2", "b", "3", "c");
        RUN(&store, "ZRANGE", "zr", "0", "-1");
        CHECK(cap_has("a") && cap_has("c"));
        RUN(&store, "ZRANGE", "zr", "0", "-1", "WITHSCORES");
        CHECK(cap_has("a"));
        RUN(&store, "ZRANGE", "zr", "1", "3", "BYSCORE", "WITHSCORES");
        CHECK(cap_has("a") || cap_has("b"));
        RUN(&store, "ZRANGE", "zr", "-2", "-1");     /* negative indices */
        CHECK(cap_has("b") || cap_has("c"));
    }

    TEST("ZRANGE error branches") {
        RUN(&store, "ZRANGE", "nozset", "0", "-1"); CHECK(cap_has("*0"));  /* missing -> empty */
        RUN(&store, "ZRANGE", "kvk", "0", "-1");    CHECK(cap()[0] == '-'); /* wrong type */
        RUN(&store, "ZRANGE", "zr", "x", "y");      CHECK(cap()[0] == '-'); /* bad int */
        RUN(&store, "ZRANGE", "zr", "a", "b", "BYSCORE"); CHECK(cap()[0] == '-'); /* bad float */
    }

    TEST("ZPOPMIN count / overflow / empty / wrongtype") {
        RUN(&store, "ZADD", "zp", "1", "a", "2", "b", "3", "c");
        RUN(&store, "ZPOPMIN", "zp", "2");
        CHECK(cap_has("a") && cap_has("b"));
        RUN(&store, "ZPOPMIN", "zp", "100");         /* count > size */
        CHECK(cap_has("c"));
        RUN(&store, "ZPOPMIN", "zp");                /* now empty -> *0 */
        CHECK(cap_has("*0"));
        RUN(&store, "ZPOPMIN", "nozset");            /* missing -> *0 */
        CHECK(cap_has("*0"));
        RUN(&store, "ZPOPMIN", "kvk", "1");          /* wrong type */
        CHECK(cap()[0] == '-');
        RUN(&store, "ZADD", "zp2", "1", "a");
        RUN(&store, "ZPOPMIN", "zp2", "notint");     /* bad count */
        CHECK(cap()[0] == '-');
    }

    /* ---- KV counter edge branches ---- */

    TEST("DECRBY + INCRBY bad-arg + INCR overflow + wrong-type") {
        RUN(&store, "SET", "cc", "100");
        RUN(&store, "DECRBY", "cc", "40"); CHECK(cap_has(":60"));
        RUN(&store, "INCRBY", "cc", "notint"); CHECK(cap()[0] == '-');
        RUN(&store, "SET", "mx", "9223372036854775807");   /* LLONG_MAX */
        RUN(&store, "INCR", "mx"); CHECK(cap()[0] == '-');  /* overflow */
        RUN(&store, "DECRBY", "cc", "-9223372036854775808"); /* LLONG_MIN delta */
        CHECK(cap()[0] == '-');
        RUN(&store, "ZADD", "zt", "1", "m");
        RUN(&store, "INCR", "zt"); CHECK(cap()[0] == '-');  /* wrong type */
        RUN(&store, "GET", "zt");  CHECK(cap()[0] == '-');  /* GET wrong type */
    }

    TF_SUITE_END();
}
