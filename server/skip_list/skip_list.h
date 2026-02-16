#ifndef SKIP_LIST_H
#define SKIP_LIST_H

#include <stddef.h>

typedef struct SkipList {
    struct Node *head;
    int max_lvl;
    size_t size;
} SkipList;
struct Node;

typedef enum {
    SL_OK,
    SL_OOM,
    SL_BAD_ARG,
    SL_DUPLICATE,
    SL_UNINITIALIZED,
    SL_NOT_FOUND,
    SL_DELETED
} SL_Status;

// Public API
SkipList* sl_create(int max_lvl);
SL_Status sl_insert(SkipList *list, char *key, void *data, size_t data_len);
void* sl_search(SkipList *list, char *key);
SL_Status sl_delete(SkipList *list, void *key);
SL_Status sl_destroy(SkipList *list);

#endif