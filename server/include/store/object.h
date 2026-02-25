#ifndef REDIS_OBJECT_H
#define REDIS_OBJECT_H

#include <stddef.h>
typedef struct SkipList SkipList;
typedef struct HashMap HashMap;

typedef enum {
    T_KV,  
    T_ZSET,
} RS_ObjType;

typedef struct RedisObject {
    RS_ObjType type; 
    char *key;       
    void *data;    // either KV value or pointer to ZSet  
    size_t data_len;
} RedisObject;

typedef struct Zset {
    SkipList *sl;
    HashMap *hm;
} Zset;

typedef struct ZSetMember {
    char *member;
    double score;
} ZSetMember;

#endif