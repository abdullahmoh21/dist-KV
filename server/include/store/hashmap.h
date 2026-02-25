#ifndef HASHMAP_H
#define HASHMAP_H

#include "object.h" 
#include <stdint.h>

#define DEFAULT_SIZE 100
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

typedef struct HashNode {
    void *val;
    struct HashNode *next;
} HashNode;

typedef struct HashMap {
    HashNode **buckets;
    size_t size;
    size_t item_count; 
    const char* (*get_key)(void *val);
} HashMap;

typedef enum {
    HM_OK,
    HM_OOM,
    HM_NOT_FOUND,
    HM_DUPLICATE,
    HM_ERR
} HM_RESULT;

HashMap* hm_create(const char* (*get_key_fn)(void *));
HM_RESULT hm_insert(HashMap *hm, void *val);
HM_RESULT hm_get(HashMap *hm, char *key, void **out);
HM_RESULT hm_delete(HashMap *hm, char *key, void **out);
HM_RESULT hm_free(HashMap *hm);
#endif