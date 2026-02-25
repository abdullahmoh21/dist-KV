#ifndef SKIP_LIST_H
#define SKIP_LIST_H

#include <stddef.h>
#include "object.h" 
#define DEFAULT_MAX_LVL 16
typedef struct SkipListNode {
    ZSetMember *obj;       
    int height;
    struct SkipListNode **forward; 
} SkipListNode;

typedef struct SkipList {
    SkipListNode *head; 
    int max_lvl;
    size_t size;
} SkipList;

typedef enum {
    SL_OK,
    SL_OOM,
    SL_BAD_ARG,
    SL_DUPLICATE,
    SL_UNINITIALIZED,
    SL_NOT_FOUND,
    SL_DELETED
} SL_RESULT;

// Public API
SkipList* sl_create(int max_lvl);
SL_RESULT sl_insert(SkipList *list, ZSetMember *obj);
ZSetMember* sl_search(SkipList *list, double score, const char *member);
SL_RESULT sl_delete(SkipList *list, double score, const char *member);
SL_RESULT sl_free(SkipList *list);

#endif