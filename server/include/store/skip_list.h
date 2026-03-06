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

typedef struct SkipListIterator {
    SkipListNode *current;
    long max;
} SkipListIterator;

typedef enum {
    SL_OK,
    SL_ERR,
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
ZSetMember* sl_search(SkipList *list, char *member, size_t member_len, double score);
SL_RESULT sl_update(SkipList *list, char *member, size_t member_len, double old_score, double new_score);
SL_RESULT sl_delete(SkipList *list, const char *member, size_t member_len, double score);
SkipListIterator sl_iterator_score(SkipList *list, double start, double end);
SkipListIterator sl_iterator_rank(SkipList *list, long start, long end);
SL_RESULT sl_free_shallow(SkipList *list);
int sl_next(SkipListIterator *it);
#endif