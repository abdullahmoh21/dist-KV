#include "store/skip_list.h"
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

// Helper prototypes
static int _random_lvl(SkipList *list);
static void _print_skiplist(SkipList *list);

// Returns < 0 if a < b, 0 if equal, > 0 if a > b 
static int zsl_cmp(ZSetMember *a, double score, const char *member, size_t member_len) {
    if (a->score < score) return -1;
    if (a->score > score) return 1;
    
    // Tie-breaker: lexicographical comparison of the strings using length
    size_t min_len = (a->key_len < member_len) ? a->key_len : member_len;
    int cmp = memcmp(a->key, member, min_len);
    if (cmp != 0) return cmp;
    
    // If prefixes match, the shorter string is "less than" the longer one
    if (a->key_len < member_len) return -1;
    if (a->key_len > member_len) return 1;
    
    return 0; 
}

SkipList* sl_create(int max_lvl){
    if(max_lvl == -1){
        max_lvl = DEFAULT_MAX_LVL;
    }
    SkipList *list = malloc(sizeof(SkipList));
    if(list == NULL){
        return NULL;
    }
    list->max_lvl = max_lvl;
    list->size = 0;
    list->head = malloc(sizeof(SkipListNode));
    if(list->head == NULL){
        free(list);
        return NULL;  
    } 

    list->head->forward = malloc(max_lvl * sizeof(SkipListNode*));   
    if(list->head->forward == NULL){
        free(list->head);
        free(list);
        return NULL;
    }
    
     list->head->obj = NULL;
     list->head->height = max_lvl;

    for(int i=0; i < max_lvl;i++){
         list->head->forward[i] = NULL;
    }
    return list;
}

SL_RESULT sl_insert(SkipList *list, ZSetMember *obj){
    if(obj == NULL || obj->key == NULL){
        return SL_ERR;
    }
    if(list == NULL || list->head == NULL) {
        return SL_ERR;
    }

    SkipListNode *current = list->head;
    SkipListNode *update[list->max_lvl];
    for(int i = list->max_lvl - 1; i >=0 ; i--){
        while(current->forward[i] != NULL && 
              zsl_cmp(current->forward[i]->obj, obj->score, obj->key, obj->key_len) < 0){
            current = current->forward[i];
        }
        update[i] = current;
    }

    // check duplicate
    SkipListNode *candidate = update[0]->forward[0];
    if(candidate != NULL && zsl_cmp(candidate->obj, obj->score, obj->key, obj->key_len) == 0){
        return SL_DUPLICATE; 
    }

    // allocate space for new Node
    SkipListNode *snode = malloc(sizeof(SkipListNode));
    if(snode == NULL){return SL_OOM;}

    int lvl = _random_lvl(list);
    if(lvl == 0){ return SL_ERR; }
    snode->forward = malloc(lvl * sizeof(SkipListNode*));
    if(snode->forward == NULL){
        free(snode);
        return SL_OOM;
    }

    for(int i=0; i<lvl;i++){
        snode->forward[i] = NULL;
    }

    snode->obj = obj;
    snode->height = lvl;

    // add to skip list
    for(int i = 0; i<lvl; i++){
        snode->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = snode;
    }
    
    list->size++;
    return SL_OK;
}

ZSetMember* sl_search(SkipList *list, char *member, size_t member_len, double score){
    if(member == NULL){
        return NULL;
    }
    if(list == NULL || list->head == NULL){
        return NULL;
    }
    
    SkipListNode *current = list->head;

    for(int i = list->max_lvl -1; i>=0; i--){
        while(current->forward[i] != NULL && 
              zsl_cmp(current->forward[i]->obj, score, member, member_len) < 0){
            current = current->forward[i];
        }
    }
    
    if(current->forward[0] != NULL && 
       zsl_cmp(current->forward[0]->obj, score, member, member_len) == 0){
        return current->forward[0]->obj;
    } else{
        return NULL;
    }
}

SL_RESULT sl_update(SkipList *list, char *member, size_t member_len, double old_score, double new_score){
    if(list == NULL || member == NULL ) return SL_ERR;
    
    SkipListNode *current = list->head;
    SkipListNode *update[list->max_lvl];
    for(int i = list->max_lvl -1; i>=0; i--){
        while(current->forward[i] != NULL && zsl_cmp(current->forward[i]->obj, old_score, member, member_len) < 0){
            current = current->forward[i];
        }
        update[i] = current;    
    }

    SkipListNode *candidate = current->forward[0];
    SkipListNode *prev = current;
    SkipListNode *next = candidate->forward[0];

    if(candidate == NULL || zsl_cmp(candidate->obj, old_score, member, member_len) != 0){
        return SL_NOT_FOUND;
    }


    // check to update in place
    bool fits_after_prev = (prev == list->head) || (zsl_cmp(prev->obj, new_score, member, member_len) < 0);
    bool fits_before_next = (next == NULL) || (zsl_cmp(next->obj, new_score, member, member_len) > 0);
    if(fits_after_prev && fits_before_next){ 
        candidate->obj->score = new_score;
        return SL_OK;
    } 

    // detach and re-insert node
    for(int i = 0; i<candidate->height; i++){
        if(update[i] != NULL){
            update[i]->forward[i] = candidate->forward[i];
        }
    }

    candidate->obj->score = new_score;

    //build new update
    current = list->head;
    for(int i = list->max_lvl -1; i>=0; i--){
        while(current->forward[i] != NULL && zsl_cmp(current->forward[i]->obj, new_score, member, member_len) < 0){
            current = current->forward[i];
        }
        update[i] = current;    
    }

    for(int i = 0; i<candidate->height; i++){
        candidate->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = candidate;
    }
    return SL_OK;
}

SL_RESULT sl_delete(SkipList *list, const char *member, size_t member_len, double score){
    if(list == NULL || list->head == NULL){
        return SL_UNINITIALIZED;
    }

    SkipListNode *current =  list->head;
    SkipListNode *update[list->max_lvl];
    memset(update, 0, sizeof(SkipListNode*) * list->max_lvl);
    
    for(int i=list->max_lvl-1; i>=0; i--){
        while(current->forward[i] != NULL && 
              zsl_cmp(current->forward[i]->obj, score, member, member_len) < 0){
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    SkipListNode *candidate = update[0]->forward[0];
    if(candidate == NULL || zsl_cmp(candidate->obj, score, member, member_len) != 0){
        return SL_NOT_FOUND;
    }
    
    for(int i = 0; i<candidate->height; i++){
        if(update[i] != NULL && update[i]->forward[i] == candidate){    //splice each lvl to skip over the candidate.
            update[i]->forward[i] = candidate->forward[i];
        }
    }
    
    // caller owns candidate->obj
    free(candidate->forward);
    free(candidate);

    if(list->size > 0) list->size--;
    return SL_DELETED;
} 

SkipListIterator sl_iterator_score(SkipList *list, double start, double end){
    SkipListIterator it = {.current = NULL, .max = -1};
    if (list == NULL || list->head == NULL) return it;
    
    // Find first node >= start
    SkipListNode *current = list->head;
    for(int i = list->max_lvl - 1; i >= 0; i--) {
        while(current->forward[i] != NULL && current->forward[i]->obj->score < start) {
            current = current->forward[i];
        }
    }
    it.current = current->forward[0];  // First node >= start
    return it;
}

SkipListIterator sl_iterator_rank(SkipList *list, long start, long end){
    SkipListIterator it = {.current = NULL, .max = end};
    if (list == NULL || list->head == NULL) return it;
    
    SkipListNode *current = list->head->forward[0];
    long rank = 0;
    
    // Skip to start position
    while(current != NULL && rank < start) {
        current = current->forward[0];
        rank++;
    }
    
    it.current = current;
    return it;
}

int sl_next(SkipListIterator *it){
    if (it == NULL || it->current == NULL) return 0;
    
    it->current = it->current->forward[0];
    
    if (it->current == NULL) return 0;
    return 1;
}

SL_RESULT sl_free_shallow(SkipList *list){
    if(list == NULL){
        return SL_OK;
    }
    SkipListNode *current = list->head;
    SkipListNode *next;
    while(current != NULL){
        next = current->forward[0];
        free(current->forward);
        free(current);
        current = next;
    }

    free(list);
    return SL_OK;
}

static int _random_lvl(SkipList *list){
    if(list == NULL){
        return 0; 
    }
    int lvl = 1;
    int promote = rand() & 1;
    while(promote == 1 && lvl < list->max_lvl){
        lvl++;
        promote = rand() & 1;
    }
    return lvl;
}

// AI-GEN pretty printer updated for ZSetMember 
static void _print_skiplist(SkipList *list) {
    if (!list || !list->head || !list->head->forward[0]) {
        printf("(skip list empty or uninitialized)\n");
        return;
    }

    int count = 0;
    for (SkipListNode *cur = list->head->forward[0]; cur; cur = cur->forward[0])
        count++;

    SkipListNode **nodes = malloc(count * sizeof(SkipListNode*));
    if (!nodes) return;

    int i = 0;
    for (SkipListNode *cur = list->head->forward[0]; cur; cur = cur->forward[0])
        nodes[i++] = cur;

    int colw = 15;
    for (i = 0; i < count; i++) {
        // AI-GEN: Calculate width based on member name and score (Length-Aware)
        int need = (int)nodes[i]->obj->key_len + 12; 
        if (need > colw) colw = need;
    }

    printf("\n--- Skip List Structure (Max Height: %d) ---\n", list->max_lvl);

    for (int lvl = list->max_lvl - 1; lvl >= 0; lvl--) {
        bool level_empty = true;
        for (i = 0; i < count; i++) {
            if (nodes[i]->height > lvl) {
                level_empty = false;
                break;
            }
        }
        
        if (level_empty && lvl > 0) continue; 

        printf("L%02d: ", lvl);
        for (i = 0; i < count; i++) {
            if (nodes[i]->height > lvl) {
                char buf[128];
                // AI-GEN: Show score and member safely with %.*s
                snprintf(buf, sizeof(buf), "%.*s:%.1f(%d)", 
                         (int)nodes[i]->obj->key_len, nodes[i]->obj->key, 
                         nodes[i]->obj->score, nodes[i]->height);
                printf("%-*s", colw, buf);
            } else {
                printf("%-*s", colw, "-");
            }
        }
        printf("\n");
    }
    printf("-------------------------------------------\n\n");

    free(nodes);
}