#include "store/skip_list.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
// Helper prototypes
static int random_lvl(SkipList *list);
static void print_skiplist(SkipList *list);

struct Node
{
    char *key;      // string based key
    void *data;     // pointer to data
    size_t key_len;
    size_t data_len;        
    struct Node **forward;
    int height;
};

int main(void) {
    srand((unsigned)time(NULL));

    SkipList *list = sl_create(16);
    // Simple payloads, just ints for testing
    int a = 10, b = 20, c = 30, d = 40, e = 50;

    sl_insert(list, "apple",  &a, sizeof(a));
    sl_insert(list, "banana", &b, sizeof(b));
    sl_insert(list, "cherry", &c, sizeof(c));
    sl_insert(list, "date",   &d, sizeof(d));
    sl_insert(list, "fig",    &e, sizeof(e));
    
    print_skiplist(list);
    
    char key[7] = "cherry\0"; 
    void *result = sl_search(list, key);
    if(result == NULL){
        printf("Key not found\n");
    } else{
        int data = *(int*) result;
        printf("Search for '%s' returned:%d\n",key,data);
    }
    
    SL_Status status = sl_delete(list, key);
    if(status == SL_NOT_FOUND){
        printf("The key '%s' does not exist\n",key);
    } else if(status == SL_DELETED){
        printf("Deleted key '%s'\n",key);
    }

    void *result2 = sl_search(list, key);
    if(result2 == NULL){
        printf("Key not found\n");
    } else{
        int data = *(int*) result2;
        printf("Search for '%s' returned:%d\n",key,data);
    }
    print_skiplist(list);

    // search()
    return 0;
}

SL_Status sl_insert(SkipList *list, char *key, void *data, size_t data_len){
    if(key == NULL || data == NULL || data_len == 0){
        return SL_BAD_ARG;
    }
    struct Node *current = list->head;
    if(list == NULL || list->head == NULL) {
        return SL_UNINITIALIZED;
    }
    struct Node *update[list->max_lvl];
    for(int i = list->max_lvl - 1; i >=0 ; i--){
        while(current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0){
            current = current->forward[i];
        }
        update[i] = current;
    }

    // check duplicate
    struct Node *candidate = update[0]->forward[0];
    if(candidate != NULL && strcmp(candidate->key, key) == 0){
        return SL_DUPLICATE; 
    }

    // allocate space for new Node
    struct Node *n = malloc(sizeof(struct Node));
    if(n == NULL){return SL_OOM;}

    n->key = malloc(strlen(key)+1);
    if(n->key == NULL){
        free(n);
        return SL_OOM;
    }

    n->data = malloc(data_len);
    if(n->data == NULL){
        free(n);
        free(n->key);
        return SL_OOM;
    }

    int lvl = random_lvl(list);
    n->forward = malloc(lvl * sizeof(struct Node*));
    if(n->forward == NULL){
        free(n);
        free(n->key);
        free(n->data);
        return SL_OOM;
    }

    for(int i=0; i<lvl;i++){
        n->forward[i] = NULL;
    }

    // copy over data
    strcpy(n->key, key);
    memcpy(n->data, data, data_len);
    n->key_len = strlen(key);
    n->data_len = data_len;
    n->height = lvl;

    // add to skip list
    for(int i = 0; i<lvl; i++){
        n->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = n;
    }
    return SL_OK;
}

SkipList* sl_create(int max_lvl){
    SkipList *list = malloc(sizeof(SkipList));
    if(list == NULL){
        return NULL;
    }
    list->max_lvl = max_lvl;
    list->head = malloc(sizeof(struct Node));
    if(list->head == NULL){
        free(list);
        return NULL;  
    } 

    list->head->forward = malloc(max_lvl * sizeof(struct Node*));   
    if(list->head->forward == NULL){
        free(list->head);
        free(list);
        return NULL;
    }
     list->head->key = NULL;
     list->head->data = NULL;
     list->head->key_len = 0;
     list->head->data_len = 0;
     list->head->height = max_lvl;

    for(int i=0; i < max_lvl;i++){
         list->head->forward[i] = NULL;
    }
    return list;
}

void* sl_search(SkipList *list, char *key){
    if(key == NULL){
        return NULL;
    }
    struct Node *current = list->head;
    if(current == NULL){
        return NULL;
    }

    for(int i = list->max_lvl -1; i>=0; i--){
        while(current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0){
            current = current->forward[i];
        }
    }
    if(current->forward[0] != NULL && strcmp(current->forward[0]->key, key) == 0){
        return current->forward[0]->data;
    } else{
        return NULL;
    }
}

SL_Status sl_delete(SkipList *list, void *key){
    if(list == NULL || list->head == NULL){
        return SL_UNINITIALIZED;
    }
    struct Node *current =  list->head;
    struct Node *update[list->max_lvl];
    memset(update, 0, sizeof(struct Node*) * list->max_lvl);
    for(int i=list->max_lvl-1; i>=0; i--){
        while(current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0){
            current = current->forward[i];
        }
        update[i] = current;
    }
    struct Node *candidate = update[0]->forward[0];
    if(candidate == NULL || strcmp(candidate->key, key) != 0){
        return SL_NOT_FOUND;
    }
    for(int i = 0; i<candidate->height; i++){
        if(update[i] != NULL && update[i]->forward[i] == candidate){    //splice each lvl to skip over the candidate.
            update[i]->forward[i] = candidate->forward[i];
        }
    }
    // deallocate the node;
    free(candidate->key);
    free(candidate->data);
    free(candidate->forward);
    free(candidate);

    return SL_DELETED;
} 

SL_Status sl_destroy(SkipList *list){
    if(list == NULL){
        return SL_UNINITIALIZED;
    }
    struct Node *current = list->head;
    struct Node *next;
    while(current != NULL){
        next = current->forward[0];
        free(current->data);
        free(current->key);
        free(current->forward);
        free(current);
        current = next;
    }

    free(list);
    return SL_OK;
}


// HELPERS
static int random_lvl(SkipList *list){
    if(list == NULL){
        return SL_UNINITIALIZED;
    }
    int lvl = 1;
    int promote = rand() & 1;
    while(promote == 1 && lvl < list->max_lvl){
        lvl++;
        promote = rand() & 1;
    }
    return lvl;
}

// AI-GEN pretty printer
static void print_skiplist(SkipList *list) {
    // Check if list or head is null, or if list is empty
    if (!list || !list->head || !list->head->forward[0]) {
        printf("(skip list empty or uninitialized)\n");
        return;
    }

    // Count nodes (level 0 walk)
    int count = 0;
    for (struct Node *cur = list->head->forward[0]; cur; cur = cur->forward[0])
        count++;

    // Temporary array to store pointers to nodes for column-based printing
    struct Node **nodes = malloc(count * sizeof(struct Node*));
    if (!nodes) return;

    int i = 0;
    for (struct Node *cur = list->head->forward[0]; cur; cur = cur->forward[0])
        nodes[i++] = cur;

    // Determine column width based on the longest key + height label
    int colw = 10;
    for (i = 0; i < count; i++) {
        int need = (int)strlen(nodes[i]->key) + 6; // +6 for "(height)"
        if (need > colw) colw = need;
    }

    printf("\n--- Skip List Structure (Max Height: %d) ---\n", list->max_lvl);

    // Tower print: Loop from the top-most possible level down to 0
    for (int lvl = list->max_lvl - 1; lvl >= 0; lvl--) {
        // Only print levels that actually have at least one node
        // (Optional: remove this check if you want to see all empty levels)
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
                snprintf(buf, sizeof(buf), "%s(%d)", nodes[i]->key, nodes[i]->height);
                printf("%-*s", colw, buf);
            } else {
                printf("%-*s", colw, "-"); // Using "-" as a placeholder for empty level slots
            }
        }
        printf("\n");
    }
    printf("-------------------------------------------\n\n");

    free(nodes);
}
