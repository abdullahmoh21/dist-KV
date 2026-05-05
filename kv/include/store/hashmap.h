#ifndef HASHMAP_H
#define HASHMAP_H

#include <stddef.h>
#include <stdint.h>

#define DEFAULT_SIZE 64
#define EXPAND_THRESH .75
#define SHRINK_THRESH .20

#if UINTPTR_MAX == 0xffffffff
    // 32-bit constants
    #define FNV_OFFSET 2166136261U
    #define FNV_PRIME 16777619U
#else
    // 64-bit constants
    #define FNV_OFFSET 14695981039346656037ULL
    #define FNV_PRIME 1099511628211ULL
#endif

typedef struct {
    char *data;
    size_t len;
} KeyView;

typedef struct HashNode {
    void *val;
    struct HashNode *next;
} HashNode;

typedef struct HashMap {
    HashNode **buckets;
    size_t size;
    size_t item_count;
    KeyView (*get_key)(void *val);
    int resize_paused;
    size_t expand_threshold;   // precomputed: size * EXPAND_THRESH, avoids float division per op
    size_t shrink_threshold;   // precomputed: size * SHRINK_THRESH
} HashMap;

typedef enum {
    HM_OK,
    HM_OOM,
    HM_NOT_FOUND,
    HM_DUPLICATE,
    HM_ERR
} HM_RESULT;

typedef struct {
    HashMap *map;
    size_t bucket_idx;
    HashNode *current_node;
} HMIterator;


HashMap* hm_create(KeyView (*get_key_fn)(void *));
HM_RESULT hm_insert(HashMap *hm, void *val);
HM_RESULT hm_get(HashMap *hm, char *key, size_t key_len, void **out);
HM_RESULT hm_delete(HashMap *hm, char *key, size_t key_len, void **out);
HM_RESULT hm_it_init(HashMap *hm, HMIterator *out_it);
HM_RESULT hm_it_next(HMIterator *it, void **out);
HM_RESULT hm_free_shallow(HashMap *hm);
void hm_pause_resize(HashMap *hm);
void hm_resume_resize(HashMap *hm);

/*
 * Find-or-insert: single hash + single chain traversal for both cases.
 * If key already exists: sets *existing_out to the found value and returns HM_OK.
 * If key is absent:     inserts new_val, sets *existing_out to NULL, returns HM_OK.
 * On allocation failure: returns HM_OOM.
 * Eliminates the double hash+traversal that occurs when callers do hm_get then hm_insert.
 */
HM_RESULT hm_find_or_insert(HashMap *hm, void *new_val, void **existing_out);

/*
 * Hash-caching variants: caller supplies the raw FNV-1a hash (from hm_compute_hash),
 * skipping the hash computation inside the function.  Use these when the same key is
 * looked up and then inserted/deleted in the same request — ZADD being the primary case
 * (_add_member does hm_get + hm_insert on the member dict for every new member).
 *
 * The hash must be the *raw* FNV value without the modulo so it remains valid across
 * resizes (the bucket index is always recomputed as hash & (hm->size - 1) at call time).
 */
size_t    hm_compute_hash(const char *key, size_t key_len);
HM_RESULT hm_get_h(HashMap *hm, const char *key, size_t key_len, size_t hash, void **out);
HM_RESULT hm_insert_h(HashMap *hm, void *val, size_t hash);
HM_RESULT hm_delete_h(HashMap *hm, const char *key, size_t key_len, size_t hash, void **out);
HM_RESULT hm_find_or_insert_h(HashMap *hm, void *new_val, size_t hash, void **existing_out);
#endif