#ifndef REDIS_STORE_H
#define REDIS_STORE_H

#include "object.h"
#include <store/hashmap.h>
#include <store/skip_list.h>
#include <parser/resp_parser.h>

enum RS_STATUS {
    RS_OK,
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

#endif