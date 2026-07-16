#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"

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

ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key   = &command->args[1];
    BulkString *value = &command->args[2];

    enum RS_RESULT set_status = rs_set(store, key, value);

    switch (set_status) {
        case RS_OK:
            sendOK(clientfd);
            return EE_OK;
        case RS_OOM:
            sendError(clientfd, "Server ran out of memory");
            return EE_OOM;
        default:
            sendError(clientfd, "Error adding value");
            return EE_ERR;
    }
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
