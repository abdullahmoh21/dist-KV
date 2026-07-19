#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "store/skip_list.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"
#include "engine/blocking.h"
#include "utils/fast_parse.h"

static ExecuteResult _parseDouble(BulkString *str, double *out) {
    if (fast_strtod(str->data, str->len, out) != 0) {
        return EE_ERR;
    }
    return EE_OK;
}

static ExecuteResult _parseLong(BulkString *str, long *out) {
    char buf[32];
    if (str->len >= sizeof(buf)) return EE_ERR;
    memcpy(buf, str->data, str->len);
    buf[str->len] = '\0';
    char *ep;
    errno = 0;
    *out = strtol(buf, &ep, 10);
    if (ep != buf + str->len || errno != 0) return EE_ERR;
    return EE_OK;
}

ExecuteResult exec_zadd(int clientfd, RedisCommand *command, RedisStore *store) {
    if (command->arg_count % 2 != 0) {
        sendError(clientfd, "Error: Odd arity for ZADD");
        return EE_OK;
    }
    BulkString *key = &command->args[1];
    int added_count = 0;
    for (int i = 2; i < command->arg_count; i += 2) {
        BulkString *score_str = &command->args[i];
        BulkString *member    = &command->args[i + 1];

        double score;
        if (_parseDouble(score_str, &score) != EE_OK) {
            sendError(clientfd, "Error parsing double");
            return EE_OK;
        }

        enum RS_RESULT status = rs_zadd(store, key, member, score);
        if (status == RS_WRONG_TYPE) {
            sendError(clientfd, "Key belongs to a key/value pair!");
            return EE_OK;
        } else if (status == RS_ERR) {
            sendError(clientfd, "Error adding a member/score pair");
            return EE_OK;
        } else if (status == RS_ADDED) {
            added_count++;
        }
    }
    sendInt(clientfd, added_count);
    return EE_OK;
}

ExecuteResult exec_zscore(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key    = &command->args[1];
    BulkString *member = &command->args[2];

    double score;
    enum RS_RESULT res = rs_zscore(store, key, member, &score);
    if (res == RS_NOT_FOUND) {
        sendNotFound(clientfd);
        return EE_OK;
    } else if (res == RS_WRONG_TYPE) {
        sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if (res == RS_ERR || res != RS_OK) {
        sendError(clientfd, "Internal Error");
        return EE_OK;
    }
    char buff[64];
    int len = snprintf(buff, sizeof(buff), "%g", score);
    sendBulkString(clientfd, buff, len);
    return EE_OK;
}

ExecuteResult exec_zrem(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key_str = &command->args[1];

    Zset *zset = NULL;
    enum RS_RESULT zset_search = rs_get_zset(store, key_str, &zset);
    if (zset_search == RS_NOT_FOUND) {
        sendInt(clientfd, 0);
        return EE_OK;
    } else if (zset_search == RS_WRONG_TYPE) {
        sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if (zset_search != RS_OK) {
        sendError(clientfd, "Internal server error");
        return EE_OK;
    }

    int removed_count = 0;
    for (int i = 2; i < command->arg_count; i++) {
        BulkString *member_str = &command->args[i];
        if (rs_zset_remove_member(zset, member_str) == RS_OK) {
            removed_count++;
        }
    }
    sendInt(clientfd, removed_count);
    return EE_OK;
}

// ZPOPMIN key [count] — atomic pop-and-remove of the `count` lowest-scored
// members (default 1). This fuses the ZRANGE-then-ZREM claim into one command,
// closing the TOCTOU race where two workers both read the same head member
// before either removes it. Single-threaded execution makes it atomic; the raw
// command replays deterministically on replicas/AOF (same lowest members).
ExecuteResult exec_zpopmin(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key_str = &command->args[1];

    long count = 1;
    if (command->arg_count >= 3) {
        if (_parseLong(&command->args[2], &count) != EE_OK || count < 0) {
            sendError(clientfd, "value is out of range, must be positive");
            return EE_OK;
        }
    }

    Zset *zset = NULL;
    enum RS_RESULT res = rs_get_zset(store, key_str, &zset);
    if (res == RS_NOT_FOUND) {
        return sendArrayHeader(clientfd, 0);
    } else if (res == RS_WRONG_TYPE) {
        sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if (res != RS_OK) {
        sendError(clientfd, "Internal server error");
        return EE_OK;
    }

    long avail  = (long)zset->sl->size;
    long to_pop = (count < avail) ? count : avail;
    if (to_pop == 0) {
        return sendArrayHeader(clientfd, 0);
    }

    // Reply is a flat array [member, score, member, score, ...].
    sendArrayHeader(clientfd, (int)(to_pop * 2));

    // Repeatedly take the current minimum (rank 0) and remove it. Removing the
    // head shifts the next-lowest into rank 0, so we re-seek each iteration
    // rather than hold an iterator across a mutation (which frees nodes).
    // The member pointer stays valid until rs_zset_remove_member frees it, so
    // we send its bytes before removing.
    for (long i = 0; i < to_pop; i++) {
        SkipListIterator it = sl_iterator_rank(zset->sl, 0, 0);
        ZSetMember *m = sl_next(&it);
        if (m == NULL) break; // defensive: size said otherwise

        sendBulkString(clientfd, m->key, m->key_len);
        char score_buf[64];
        int score_len = snprintf(score_buf, sizeof(score_buf), "%g", m->score);
        sendBulkString(clientfd, score_buf, score_len);

        BulkString member_bs = { .data = m->key, .len = m->key_len };
        rs_zset_remove_member(zset, &member_bs);
    }

    // Redis convention: an emptied sorted set deletes its key.
    if (zset->sl->size == 0) {
        rs_delete(store, key_str);
    }

    return EE_OK;
}

// ---------------------------------------------------------------------------
// BZPOPMIN — blocking claim
// ---------------------------------------------------------------------------

// Scratch for the rewritten `ZPOPMIN <key>` propagation frame. Both callers
// (the handler and the server's parked-serve path) consume it before the next
// serve can happen, so a single static buffer is safe on this single-threaded
// event loop.
static char  *g_bpop_frame = NULL;
static size_t g_bpop_cap   = 0;

static int _bpop_reserve(size_t need) {
    if (g_bpop_cap >= need) return 1;
    size_t cap = g_bpop_cap ? g_bpop_cap : 128;
    while (cap < need) cap *= 2;
    char *nb = realloc(g_bpop_frame, cap);
    if (!nb) return 0;
    g_bpop_frame = nb;
    g_bpop_cap   = cap;
    return 1;
}

// Build `*2\r\n$7\r\nZPOPMIN\r\n$<len>\r\n<key>\r\n` into the scratch buffer.
static int _build_zpopmin_frame(const BulkString *key, const char **out, size_t *out_len) {
    char hdr[32];
    int hlen = snprintf(hdr, sizeof(hdr), "$%zu\r\n", key->len);
    if (hlen < 0) return 0;

    size_t need = 4 + 13 + (size_t)hlen + key->len + 2;
    if (!_bpop_reserve(need)) return 0;

    char *p = g_bpop_frame;
    memcpy(p, "*2\r\n$7\r\nZPOPMIN\r\n", 17); p += 17;
    memcpy(p, hdr, (size_t)hlen);            p += hlen;
    memcpy(p, key->data, key->len);          p += key->len;
    memcpy(p, "\r\n", 2);                    p += 2;

    *out     = g_bpop_frame;
    *out_len = (size_t)(p - g_bpop_frame);
    return 1;
}

int zset_serve_blocking_pop(int clientfd, RedisStore *store,
                            const BulkString *keys, int nkeys,
                            const char **out_frame, size_t *out_len) {
    for (int i = 0; i < nkeys; i++) {
        BulkString key = keys[i];   // rs_* take a non-const BulkString*
        Zset *zset = NULL;
        enum RS_RESULT r = rs_get_zset(store, &key, &zset);

        if (r == RS_WRONG_TYPE) {
            sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
            return -1;
        }
        if (r != RS_OK || zset->sl->size == 0) continue;   // absent/expired/empty -> try next key

        SkipListIterator it = sl_iterator_rank(zset->sl, 0, 0);
        ZSetMember *m = sl_next(&it);
        if (m == NULL) continue;  // defensive: size said otherwise

        // Build the propagation frame before the member is freed — it borrows
        // key bytes, which for a parked client live in the PendingPop copy.
        if (!_build_zpopmin_frame(&key, out_frame, out_len)) {
            sendError(clientfd, "Server ran out of memory");
            return -1;
        }

        // Reply reads m->key, so send before rs_zset_remove_member frees it.
        sendArrayHeader(clientfd, 3);
        sendBulkString(clientfd, key.data, key.len);
        sendBulkString(clientfd, m->key, m->key_len);
        char score_buf[64];
        int score_len = snprintf(score_buf, sizeof(score_buf), "%g", m->score);
        sendBulkString(clientfd, score_buf, score_len);

        BulkString member_bs = { .data = m->key, .len = m->key_len };
        rs_zset_remove_member(zset, &member_bs);

        // Redis convention: an emptied sorted set deletes its key.
        if (zset->sl->size == 0) rs_delete(store, &key);
        return 1;
    }
    return 0;
}

// BZPOPMIN key [key ...] timeout   (timeout in seconds, may be fractional; 0 = forever)
//
// Serves immediately when any key has a member. Otherwise returns EE_POP_PENDING
// and server.c parks the client until a producer ZADDs or the deadline passes.
ExecuteResult exec_bzpopmin(int clientfd, RedisCommand *command, RedisStore *store) {
    int nkeys = command->arg_count - 2;   // argv[0]=BZPOPMIN, argv[last]=timeout
    if (nkeys < 1) {
        sendError(clientfd, "Wrong number of arguments");
        return EE_ERR;
    }

    double timeout;
    if (_parseDouble(&command->args[command->arg_count - 1], &timeout) != EE_OK) {
        sendError(clientfd, "timeout is not a float or out of range");
        return EE_ERR;
    }
    if (timeout < 0) {
        sendError(clientfd, "timeout is negative");
        return EE_ERR;
    }

    const char *frame = NULL;
    size_t flen = 0;
    int served = zset_serve_blocking_pop(clientfd, store, &command->args[1], nkeys, &frame, &flen);
    if (served < 0) return EE_ERR;      // error already replied; never propagate
    if (served == 0) return EE_POP_PENDING;

    // Propagate the effect (`ZPOPMIN key`), never the blocking frame.
    command->raw_start = (char *)frame;
    command->raw_len   = flen;
    return EE_OK;
}

ExecuteResult exec_zrange(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key_str   = &command->args[1];
    BulkString *start_str = &command->args[2];
    BulkString *stop_str  = &command->args[3];

    int by_score    = 0;
    int with_scores = 0;
    for (int i = 4; i < command->arg_count; i++) {
        BulkString *flag = &command->args[i];
        if (strncasecmp(flag->data, "BYSCORE", flag->len) == 0) {
            by_score = 1;
        } else if (strncasecmp(flag->data, "WITHSCORES", flag->len) == 0) {
            with_scores = 1;
        }
    }

    Zset *zset = NULL;
    enum RS_RESULT zset_res = rs_get_zset(store, key_str, &zset);
    if (zset_res == RS_NOT_FOUND) {
        return sendArrayHeader(clientfd, 0);
    } else if (zset_res == RS_WRONG_TYPE) {
        sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if (zset_res != RS_OK) {
        sendError(clientfd, "Internal server error");
        return EE_OK;
    }

    SkipListIterator it;
    if (by_score) {
        double start, stop;
        if (_parseDouble(start_str, &start) != EE_OK || _parseDouble(stop_str, &stop) != EE_OK) {
            sendError(clientfd, "ERR value is not a valid float");
            return EE_OK;
        }
        it = sl_iterator_score(zset->sl, start, stop);
    } else {
        long start, stop;
        if (_parseLong(start_str, &start) != EE_OK || _parseLong(stop_str, &stop) != EE_OK) {
            sendError(clientfd, "ERR value is not an integer or out of range");
            return EE_OK;
        }
        long size = (long)zset->sl->size;
        if (start < 0) start = size + start;
        if (stop  < 0) stop  = size + stop;
        if (start < 0) start = 0;
        if (stop >= size) stop = size - 1;
        it = sl_iterator_rank(zset->sl, start, stop);
    }

    // Pass 1: count result set size (iterator is a stack struct — copy is free)
    SkipListIterator count_it = it;
    int count = 0;
    while (sl_next(&count_it) != NULL) count++;

    ExecuteResult res = sendArrayHeader(clientfd, with_scores ? count * 2 : count);
    if (res != EE_OK) return res;

    // Pass 2: send members (and optionally scores)
    ZSetMember *member;
    while ((member = sl_next(&it)) != NULL) {
        res = sendBulkString(clientfd, member->key, member->key_len);
        if (res != EE_OK) return res;
        if (with_scores) {
            char score_buf[64];
            int score_len = snprintf(score_buf, sizeof(score_buf), "%g", member->score);
            res = sendBulkString(clientfd, score_buf, score_len);
            if (res != EE_OK) return res;
        }
    }

    return EE_OK;
}
