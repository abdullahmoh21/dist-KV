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
#endif