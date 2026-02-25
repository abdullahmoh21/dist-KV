#include "store/skip_list.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

// Helper prototypes
static int random_lvl(SkipList *list);
static void print_skiplist(SkipList *list);

// Returns < 0 if a < b, 0 if equal, > 0 if a > b
static int zsl_cmp(ZSetMember *a, double score, const char *member) {
    if (a->score < score) return -1;
    if (a->score > score) return 1;
    return strcmp(a->member, member);
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
    if(obj == NULL || obj->member == NULL){
        return SL_BAD_ARG;
    }
    if(list == NULL || list->head == NULL) {
        return SL_UNINITIALIZED;
    }

    SkipListNode *current = list->head;
    SkipListNode *update[list->max_lvl];
    for(int i = list->max_lvl - 1; i >=0 ; i--){
        // AI-GEN: Use our 2D comparison helper
        while(current->forward[i] != NULL && 
              zsl_cmp(current->forward[i]->obj, obj->score, obj->member) < 0){
            current = current->forward[i];
        }
        update[i] = current;
    }

    // check duplicate
    SkipListNode *candidate = update[0]->forward[0];
    if(candidate != NULL && zsl_cmp(candidate->obj, obj->score, obj->member) == 0){
        return SL_DUPLICATE; 
    }

    // allocate space for new Node
    SkipListNode *n = malloc(sizeof(SkipListNode));
    if(n == NULL){return SL_OOM;}

    int lvl = random_lvl(list);
    if(lvl == 0){ return SL_UNINITIALIZED; }
    n->forward = malloc(lvl * sizeof(SkipListNode*));
    if(n->forward == NULL){
        free(n);
        return SL_OOM;
    }

    for(int i=0; i<lvl;i++){
        n->forward[i] = NULL;
    }

    // copy over data (Now just pointing to the overarching ZSetMember)
    n->obj = obj;
    n->height = lvl;

    // add to skip list
    for(int i = 0; i<lvl; i++){
        n->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = n;
    }
    
    list->size++;
    return SL_OK;
}

ZSetMember* sl_search(SkipList *list, double score, const char *member){
    if(member == NULL){
        return NULL;
    }
    if(list == NULL || list->head == NULL){
        return NULL;
    }
    
    SkipListNode *current = list->head;

    for(int i = list->max_lvl -1; i>=0; i--){
        while(current->forward[i] != NULL && 
              zsl_cmp(current->forward[i]->obj, score, member) < 0){
            current = current->forward[i];
        }
    }
    
    if(current->forward[0] != NULL && 
       zsl_cmp(current->forward[0]->obj, score, member) == 0){
        return current->forward[0]->obj;
    } else{
        return NULL;
    }
}

SL_RESULT sl_delete(SkipList *list, double score, const char *member){
    if(list == NULL || list->head == NULL){
        return SL_UNINITIALIZED;
    }

    SkipListNode *current =  list->head;
    SkipListNode *update[list->max_lvl];
    memset(update, 0, sizeof(SkipListNode*) * list->max_lvl);
    
    for(int i=list->max_lvl-1; i>=0; i--){
        while(current->forward[i] != NULL && 
              zsl_cmp(current->forward[i]->obj, score, member) < 0){
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    SkipListNode *candidate = update[0]->forward[0];
    if(candidate == NULL || zsl_cmp(candidate->obj, score, member) != 0){
        return SL_NOT_FOUND;
    }
    
    for(int i = 0; i<candidate->height; i++){
        if(update[i] != NULL && update[i]->forward[i] == candidate){    //splice each lvl to skip over the candidate.
            update[i]->forward[i] = candidate->forward[i];
        }
    }
    
    // BUG FIX 2: Deallocate ONLY the node, not the ZSetMember contents.
    free(candidate->forward);
    free(candidate);

    if(list->size > 0) list->size--;
    return SL_DELETED;
} 

SL_RESULT sl_free(SkipList *list){
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

static int random_lvl(SkipList *list){
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
static void print_skiplist(SkipList *list) {
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
        // AI-GEN: Calculate width based on member name and score
        int need = (int)strlen(nodes[i]->obj->member) + 12; 
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
                // AI-GEN: Show score and member
                snprintf(buf, sizeof(buf), "%s:%.1f(%d)", nodes[i]->obj->member, nodes[i]->obj->score, nodes[i]->height);
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