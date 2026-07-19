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

// Strict int64 parse: the whole token must be a valid integer, no leading
// whitespace or trailing junk (Redis INCR semantics).
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

// Shared INCR/DECR/INCRBY/DECRBY core. Reads the current value, applies delta
// with overflow protection, writes it back, and replies with the new value.
// INCR replays deterministically through AOF/replication (same command order →
// same result), so forwarding the raw command is safe.
static ExecuteResult _apply_incr(int clientfd, RedisCommand *command, RedisStore *store, long long delta) {
    BulkString *key = &command->args[1];

    RedisObject *obj = NULL;
    long long cur = 0;
    enum RS_RESULT res = rs_get(store, key, &obj);
    if (res == RS_OK) {
        if (_parse_i64((const char *)obj->data, obj->data_len, &cur) != 0) {
            sendError(clientfd, "value is not an integer or out of range");
            return EE_OK;
        }
    } else if (res == RS_WRONG_TYPE) {
        sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if (res != RS_NOT_FOUND) {
        sendError(clientfd, "error fetching key");
        return EE_OK;
    }

    if ((delta > 0 && cur > LLONG_MAX - delta) ||
        (delta < 0 && cur < LLONG_MIN - delta)) {
        sendError(clientfd, "increment or decrement would overflow");
        return EE_OK;
    }
    long long next = cur + delta;

    char buf[24];
    int n = snprintf(buf, sizeof(buf), "%lld", next);
    BulkString val = { .data = buf, .len = (size_t)n };
    if (rs_set(store, key, &val) != RS_OK) {
        sendError(clientfd, "error storing value");
        return EE_ERR;
    }
    sendInt64(clientfd, next);
    return EE_OK;
}

ExecuteResult exec_incr(int clientfd, RedisCommand *command, RedisStore *store) {
    return _apply_incr(clientfd, command, store, 1);
}

ExecuteResult exec_decr(int clientfd, RedisCommand *command, RedisStore *store) {
    return _apply_incr(clientfd, command, store, -1);
}

ExecuteResult exec_incrby(int clientfd, RedisCommand *command, RedisStore *store) {
    long long delta;
    if (_parse_i64((const char *)command->args[2].data, command->args[2].len, &delta) != 0) {
        sendError(clientfd, "value is not an integer or out of range");
        return EE_OK;
    }
    return _apply_incr(clientfd, command, store, delta);
}

ExecuteResult exec_decrby(int clientfd, RedisCommand *command, RedisStore *store) {
    long long delta;
    if (_parse_i64((const char *)command->args[2].data, command->args[2].len, &delta) != 0) {
        sendError(clientfd, "value is not an integer or out of range");
        return EE_OK;
    }
    // LLONG_MIN can't be negated without overflow; reject up front.
    if (delta == LLONG_MIN) {
        sendError(clientfd, "decrement would overflow");
        return EE_OK;
    }
    return _apply_incr(clientfd, command, store, -delta);
}

// Case-insensitive compare of a token against a literal option name.
static int _opt_is(const BulkString *b, const char *lit, size_t litlen) {
    return b->len == litlen && strncasecmp(b->data, lit, litlen) == 0;
}

// --- Propagation-frame rewrite (the Phase-2c trap, for SET options) ---
//
// aof_add / repl_propagate serialize command->raw_start/raw_len verbatim. A raw
// `SET k v NX EX 100` is wrong to persist twice over: `EX 100` would replay
// relative to *replay* time (drifting the deadline), and `NX`/`XX` would re-run
// their condition against replica/replay state — worse, aof_load treats an NX
// no-op as a fatal AOF_EXEC_ERR. So on any successful option-SET we rewrite the
// propagated frame to the canonical effect: `SET key value` (+ absolute
// `PEXPIREAT key <abs-ms>` when a TTL was set). The scratch buffer is reused
// safely: the single-threaded main loop copies it into the AOF/backlog before
// the next command is dispatched.
static char  *g_set_rw_buf = NULL;
static size_t g_set_rw_cap = 0;

static int _set_rw_reserve(size_t need) {
    if (g_set_rw_cap >= need) return 1;
    size_t nc = g_set_rw_cap ? g_set_rw_cap * 2 : 256;
    while (nc < need) nc *= 2;
    char *nb = realloc(g_set_rw_buf, nc);
    if (!nb) return 0;
    g_set_rw_buf = nb;
    g_set_rw_cap = nc;
    return 1;
}

// Append one RESP bulk string ("$<len>\r\n<data>\r\n") at *p, advancing it.
static char *_put_bulk(char *p, const char *s, size_t n) {
    char lenbuf[24];
    int ll = snprintf(lenbuf, sizeof(lenbuf), "%zu", n);
    *p++ = '$';
    memcpy(p, lenbuf, ll); p += ll;
    *p++ = '\r'; *p++ = '\n';
    memcpy(p, s, n); p += n;
    *p++ = '\r'; *p++ = '\n';
    return p;
}

// Rewrite command's raw frame to canonical `SET key value` plus, if has_ttl,
// `PEXPIREAT key abs_ms`. Returns 1 on success, 0 on OOM (frame left untouched).
static int _rewrite_set_frame(RedisCommand *command, const BulkString *key,
                              const BulkString *value, int has_ttl, uint64_t abs_ms) {
    char absbuf[24];
    int abslen = snprintf(absbuf, sizeof(absbuf), "%llu", (unsigned long long)abs_ms);

    // Generous upper bound: two 3-elem arrays, fixed cmd tokens, two keys, value.
    size_t need = 256 + 2 * key->len + value->len + (size_t)abslen;
    if (!_set_rw_reserve(need)) return 0;

    char *p = g_set_rw_buf;
    memcpy(p, "*3\r\n", 4); p += 4;
    p = _put_bulk(p, "SET", 3);
    p = _put_bulk(p, key->data, key->len);
    p = _put_bulk(p, value->data, value->len);

    if (has_ttl) {
        memcpy(p, "*3\r\n", 4); p += 4;
        p = _put_bulk(p, "PEXPIREAT", 9);
        p = _put_bulk(p, key->data, key->len);
        p = _put_bulk(p, absbuf, (size_t)abslen);
    }

    command->raw_start = g_set_rw_buf;
    command->raw_len   = (size_t)(p - g_set_rw_buf);
    return 1;
}

// Saturating "now + delta_ms" absolute epoch ms (delta is always > 0 here).
static uint64_t _set_deadline(long long delta_ms) {
    uint64_t now = wallclock_ms();
    uint64_t pos = (uint64_t)delta_ms;
    if (pos > UINT64_MAX - now) return UINT64_MAX;
    return now + pos;
}

ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key   = &command->args[1];
    BulkString *value = &command->args[2];

    // --- parse trailing options: [NX|XX] [GET] [EX sec | PX ms] (order-free) ---
    int has_nx = 0, has_xx = 0, has_get = 0;
    int ttl_unit = 0;              // 0 none, 1 EX (sec), 2 PX (ms)
    long long ttl_amount = 0;

    for (int i = 3; i < command->arg_count; i++) {
        BulkString *o = &command->args[i];
        if (_opt_is(o, "NX", 2)) {
            if (has_xx) { sendError(clientfd, "syntax error"); return EE_ERR; }
            has_nx = 1;
        } else if (_opt_is(o, "XX", 2)) {
            if (has_nx) { sendError(clientfd, "syntax error"); return EE_ERR; }
            has_xx = 1;
        } else if (_opt_is(o, "GET", 3)) {
            has_get = 1;
        } else if (_opt_is(o, "EX", 2) || _opt_is(o, "PX", 2)) {
            if (ttl_unit) { sendError(clientfd, "syntax error"); return EE_ERR; }
            int is_ms = _opt_is(o, "PX", 2);
            if (i + 1 >= command->arg_count) { sendError(clientfd, "syntax error"); return EE_ERR; }
            i++;
            if (_parse_i64((const char *)command->args[i].data, command->args[i].len, &ttl_amount) != 0) {
                sendError(clientfd, "value is not an integer or out of range");
                return EE_ERR;
            }
            if (ttl_amount <= 0) {
                sendError(clientfd, "invalid expire time in 'set' command");
                return EE_ERR;
            }
            ttl_unit = is_ms ? 2 : 1;
        } else {
            sendError(clientfd, "syntax error");
            return EE_ERR;
        }
    }

    // Convert TTL to an absolute deadline (needed for both apply + propagation).
    long long delta_ms = 0;
    uint64_t abs_ms = 0;
    if (ttl_unit) {
        if (ttl_unit == 1) {
            if (ttl_amount > LLONG_MAX / 1000) {
                sendError(clientfd, "invalid expire time in 'set' command");
                return EE_ERR;
            }
            delta_ms = ttl_amount * 1000;
        } else {
            delta_ms = ttl_amount;
        }
        abs_ms = _set_deadline(delta_ms);
    }

    // GET: fetch (and type-check) the old value up front. Reply carries it either
    // way — whether or not the write ends up happening.
    RedisObject *oldobj = NULL;
    int have_old = 0;
    if (has_get) {
        enum RS_RESULT gr = rs_get(store, key, &oldobj);
        if (gr == RS_WRONG_TYPE) {
            sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
            return EE_ERR;
        }
        have_old = (gr == RS_OK);
    }

    // Presence probe honoring lazy expiry: -2 == absent, anything else == present
    // (any type). Drives the NX/XX conditional.
    int exists = (rs_key_ttl_ms(store, key) != -2);
    int skip_write = (has_nx && exists) || (has_xx && !exists);

    // Emit the reply now — for GET this reads oldobj->data before rs_set can free it.
    if (has_get) {
        if (have_old) sendBulkString(clientfd, (const char *)oldobj->data, oldobj->data_len);
        else          sendNotFound(clientfd);
    }

    if (skip_write) {
        if (!has_get) sendNotFound(clientfd);   // NX/XX failure without GET -> null
        return EE_OK_NO_PROP;                    // condition unmet: nothing written, don't propagate
    }

    enum RS_RESULT set_status = rs_set(store, key, value);
    if (set_status == RS_OOM) {
        if (!has_get) sendError(clientfd, "Server ran out of memory");
        return EE_OOM;
    } else if (set_status != RS_OK) {
        if (!has_get) sendError(clientfd, "Error adding value");
        return EE_ERR;
    }

    // rs_set clears any prior TTL; re-apply the requested one on the fresh object.
    if (ttl_unit) rs_set_expire(store, key, abs_ms);

    if (!has_get) sendOK(clientfd);

    // Options present -> propagate the canonical effect, not the raw conditional
    // frame. Plain `SET k v` (arg_count == 3) forwards its own frame untouched.
    if (command->arg_count > 3) {
        _rewrite_set_frame(command, key, value, ttl_unit != 0, abs_ms);
    }
    return EE_OK;
}

ExecuteResult exec_get(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key = &command->args[1];

    RedisObject *obj;
    enum RS_RESULT res = rs_get(store, key, &obj);
    if (res == RS_NOT_FOUND) {
        sendNotFound(clientfd);
        return EE_OK;
    } else if (res == RS_WRONG_TYPE) {
        sendError(clientfd, "Incorrect type. GET only works on key/value pairs");
        return EE_OK;
    } else if (res == RS_ERR || res != RS_OK) {
        sendError(clientfd, "Error fetching key");
        return EE_OK;
    }
    sendBulkString(clientfd, (const char *)obj->data, obj->data_len);
    return EE_OK;
}

ExecuteResult exec_del(int clientfd, RedisCommand *command, RedisStore *store) {
    int del_count = 0;
    for (int i = 1; i < command->arg_count; i++) {
        BulkString *key = &command->args[i];
        if (rs_delete(store, key) == RS_OK) {
            del_count++;
        }
    }
    return sendInt(clientfd, del_count);
}
