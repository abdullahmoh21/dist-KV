#ifndef REDIS_STORE_H
#define REDIS_STORE_H

#include "object.h"
#include <store/hashmap.h>
#include <store/skip_list.h>
#include <parser/resp_parser.h>


enum RS_RESULT {
    RS_OK,
    RS_ADDED,
    RS_UPDATED,
    RS_NOT_FOUND,
    RS_DUPLICATE,
    RS_ERR,
    RS_WRONG_TYPE,
    RS_BAD_ARG,
    RS_OOM
};

typedef struct RedisStore {
    HashMap *dict;
} RedisStore;

typedef struct RS_ZIterator{
    SkipListIterator internal_it;
} RS_ZIterator;

enum RS_RESULT create_store(RedisStore *store);
enum RS_RESULT rs_get(RedisStore *store, BulkString *key_str, RedisObject **out);
enum RS_RESULT rs_set(RedisStore *store, BulkString *key_str, BulkString *data_str);
enum RS_RESULT rs_delete(RedisStore *store, BulkString *key_str);
enum RS_RESULT rs_zadd(RedisStore *store, BulkString *zkey_str, BulkString *member, double score);
enum RS_RESULT rs_zscore(RedisStore *store, BulkString *key_str, BulkString *member_str, double *out);
enum RS_RESULT rs_get_zset(RedisStore *store, BulkString *key_str, Zset **out);
enum RS_RESULT rs_zset_remove_member(Zset *zset, BulkString *member_str);
enum RS_RESULT rs_zrange_score_init(RedisStore *store, BulkString *key, double min, double max, RS_ZIterator *out_it);
ZSetMember* rs_ziterator_next(RS_ZIterator *it);
#endif