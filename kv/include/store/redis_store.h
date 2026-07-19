#ifndef REDIS_STORE_H
#define REDIS_STORE_H

#include "object.h"
#include <parser/resp_parser.h>
#include <stddef.h>

#ifndef AOF_BUF_H
typedef struct AOFManager AOFManager;
#endif

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
    AOFManager *aof;
    size_t active_expire_cursor; // rotating bucket index for the active-expiry sweep
} RedisStore;

// Invoked once per key the active-expiry sweep evicts, BEFORE the object is freed.
// The main loop uses it to propagate a DEL to the AOF/replicas so persisted and
// replicated state stays consistent with what the master reaped.
typedef void (*rs_expire_cb)(const char *key, size_t key_len, void *ctx);

enum RS_RESULT create_store(RedisStore *store);
enum RS_RESULT rs_flush(RedisStore *store);
enum RS_RESULT rs_get(RedisStore *store, BulkString *key_str, RedisObject **out);
enum RS_RESULT rs_set(RedisStore *store, BulkString *key_str, BulkString *data_str);
enum RS_RESULT rs_delete(RedisStore *store, BulkString *key_str);
enum RS_RESULT rs_zadd(RedisStore *store, BulkString *zkey_str, BulkString *member, double score);
enum RS_RESULT rs_zscore(RedisStore *store, BulkString *key_str, BulkString *member_str, double *out);
enum RS_RESULT rs_get_zset(RedisStore *store, BulkString *key_str, Zset **out);
enum RS_RESULT rs_zset_remove_member(Zset *zset, BulkString *member_str);

// --- Expiry (Phase 2) ---

// Set an absolute Unix-epoch-ms deadline on an existing key.
// RS_OK if the key exists (TTL set/overwritten), RS_NOT_FOUND if absent/expired.
enum RS_RESULT rs_set_expire(RedisStore *store, BulkString *key_str, uint64_t expire_at_ms);

// Remove any TTL from a key. *removed = 1 if a TTL was cleared, 0 otherwise.
// RS_OK if the key exists, RS_NOT_FOUND if absent/expired.
enum RS_RESULT rs_persist(RedisStore *store, BulkString *key_str, int *removed);

// Milliseconds of TTL left for a key:
//   -2  key missing (or lazily expired by this call)
//   -1  key exists with no TTL
//  >=0  ms remaining until the deadline
long long rs_key_ttl_ms(RedisStore *store, BulkString *key_str);

// Sample a rotating window of buckets and evict keys whose deadline has passed.
// Calls cb(key, key_len, ctx) for each evicted key before freeing it.
// Returns the number of keys evicted this cycle.
int rs_active_expire_cycle(RedisStore *store, uint64_t now_ms, rs_expire_cb cb, void *ctx);
#endif