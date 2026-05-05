#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "store/skip_list.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"
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
