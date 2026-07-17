#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"
#include "utils/time.h"

// Strict int64 parse — the whole token must be a valid integer (Redis semantics).
static int _parse_i64(const char *s, size_t len, long long *out) {
    if (len == 0 || len > 20) return -1;
    if (s[0] == ' ' || s[0] == '\t') return -1;
    char buf[24];
    memcpy(buf, s, len);
    buf[len] = '\0';
    char *end;
    errno = 0;
    long long v = strtoll(buf, &end, 10);
    if (errno != 0 || end != buf + len) return -1;
    *out = v;
    return 0;
}

// --- Command rewrite (the 2c trap) ---
//
// aof_add and repl_propagate both serialize command->raw_start/raw_len (the raw
// RESP frame), NOT the parsed args. A verbatim `EXPIRE k 100` would replay as
// "100s from *replay* time" on AOF restart and on replicas — drifting the
// deadline. So on success we rewrite the frame to an absolute `PEXPIREAT key
// <abs-ms>` in this scratch buffer and repoint raw_start/raw_len at it. The
// buffer is safe to reuse: the main loop copies it into the AOF/backlog
// synchronously (single-threaded) before the next command is dispatched.
static char  *g_rw_buf = NULL;
static size_t g_rw_cap = 0;

static int _rw_reserve(size_t need) {
    if (g_rw_cap >= need) return 1;
    size_t nc = g_rw_cap ? g_rw_cap * 2 : 256;
    while (nc < need) nc *= 2;
    char *nb = realloc(g_rw_buf, nc);
    if (!nb) return 0;
    g_rw_buf = nb;
    g_rw_cap = nc;
    return 1;
}

// Returns 1 on success (command repointed), 0 on OOM (command left untouched).
static int _rewrite_to_pexpireat(RedisCommand *command, const char *key, size_t klen, uint64_t abs_ms) {
    char valbuf[24];
    int vlen = snprintf(valbuf, sizeof(valbuf), "%llu", (unsigned long long)abs_ms);

    char klenbuf[24]; int klenlen = snprintf(klenbuf, sizeof(klenbuf), "%zu", klen);
    char vlenbuf[24]; int vlenlen = snprintf(vlenbuf, sizeof(vlenbuf), "%d", vlen);

    // "*3\r\n" + "$9\r\nPEXPIREAT\r\n" + "$<klen>\r\n<key>\r\n" + "$<vlen>\r\n<val>\r\n"
    size_t need = 4 + 15
                + 1 + (size_t)klenlen + 2 + klen + 2
                + 1 + (size_t)vlenlen + 2 + (size_t)vlen + 2;
    if (!_rw_reserve(need)) return 0;

    char *p = g_rw_buf;
    #define RW_PUT(s, n) do { memcpy(p, (s), (n)); p += (n); } while (0)
    RW_PUT("*3\r\n", 4);
    RW_PUT("$9\r\nPEXPIREAT\r\n", 15);
    *p++ = '$'; RW_PUT(klenbuf, klenlen); RW_PUT("\r\n", 2);
    RW_PUT(key, klen);                    RW_PUT("\r\n", 2);
    *p++ = '$'; RW_PUT(vlenbuf, vlenlen); RW_PUT("\r\n", 2);
    RW_PUT(valbuf, vlen);                 RW_PUT("\r\n", 2);
    #undef RW_PUT

    command->raw_start = g_rw_buf;
    command->raw_len   = (size_t)(p - g_rw_buf);
    return 1;
}

// Saturating "now + delta_ms" in absolute epoch ms. Negative delta (past TTL)
// clamps to 0; overflow clamps to UINT64_MAX.
static uint64_t _deadline_from_delta(long long delta_ms) {
    uint64_t now = wallclock_ms();
    if (delta_ms < 0) {
        uint64_t neg = (uint64_t)(-delta_ms);
        return (neg >= now) ? 0 : (now - neg);
    }
    uint64_t pos = (uint64_t)delta_ms;
    if (pos > UINT64_MAX - now) return UINT64_MAX;
    return now + pos;
}

// Shared EXPIRE (seconds) / PEXPIRE (ms) core.
static ExecuteResult _expire_relative(int clientfd, RedisCommand *command, RedisStore *store, int is_ms) {
    BulkString *key = &command->args[1];

    long long amount;
    if (_parse_i64((const char *)command->args[2].data, command->args[2].len, &amount) != 0) {
        sendError(clientfd, "value is not an integer or out of range");
        return EE_OK;
    }

    long long delta_ms;
    if (is_ms) {
        delta_ms = amount;
    } else {
        if (amount > LLONG_MAX / 1000 || amount < LLONG_MIN / 1000) {
            sendError(clientfd, "invalid expire time");
            return EE_OK;
        }
        delta_ms = amount * 1000;
    }

    uint64_t abs_ms = _deadline_from_delta(delta_ms);

    enum RS_RESULT res = rs_set_expire(store, key, abs_ms);
    if (res == RS_OK) {
        // Rewrite to absolute PEXPIREAT for deterministic AOF/replica replay.
        _rewrite_to_pexpireat(command, (const char *)key->data, key->len, abs_ms);
        sendInt(clientfd, 1);
    } else if (res == RS_NOT_FOUND) {
        sendInt(clientfd, 0);
    } else {
        sendError(clientfd, "error setting expiry");
    }
    return EE_OK;
}

ExecuteResult exec_expire(int clientfd, RedisCommand *command, RedisStore *store) {
    return _expire_relative(clientfd, command, store, 0);
}

ExecuteResult exec_pexpire(int clientfd, RedisCommand *command, RedisStore *store) {
    return _expire_relative(clientfd, command, store, 1);
}

// PEXPIREAT is already absolute — no rewrite; the raw frame replays deterministically.
ExecuteResult exec_pexpireat(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key = &command->args[1];

    long long abs;
    if (_parse_i64((const char *)command->args[2].data, command->args[2].len, &abs) != 0) {
        sendError(clientfd, "value is not an integer or out of range");
        return EE_OK;
    }
    uint64_t abs_ms = (abs < 0) ? 0 : (uint64_t)abs;

    enum RS_RESULT res = rs_set_expire(store, key, abs_ms);
    if (res == RS_OK) {
        sendInt(clientfd, 1);
    } else if (res == RS_NOT_FOUND) {
        sendInt(clientfd, 0);
    } else {
        sendError(clientfd, "error setting expiry");
    }
    return EE_OK;
}

ExecuteResult exec_ttl(int clientfd, RedisCommand *command, RedisStore *store) {
    long long ms = rs_key_ttl_ms(store, &command->args[1]);
    if (ms < 0) {
        sendInt64(clientfd, ms);            // -1 (no TTL) or -2 (missing)
    } else {
        sendInt64(clientfd, (ms + 500) / 1000);  // round to nearest second, Redis-style
    }
    return EE_OK;
}

ExecuteResult exec_pttl(int clientfd, RedisCommand *command, RedisStore *store) {
    sendInt64(clientfd, rs_key_ttl_ms(store, &command->args[1]));
    return EE_OK;
}

ExecuteResult exec_persist(int clientfd, RedisCommand *command, RedisStore *store) {
    int removed = 0;
    enum RS_RESULT res = rs_persist(store, &command->args[1], &removed);
    if (res == RS_NOT_FOUND) {
        sendInt(clientfd, 0);
    } else if (res == RS_OK) {
        sendInt(clientfd, removed);
    } else {
        sendError(clientfd, "error persisting key");
    }
    return EE_OK;
}
