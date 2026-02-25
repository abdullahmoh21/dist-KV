#include "store/hashmap.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>


HashMap* hm_create(const char* (*get_key_fn)(void *)){
    HashMap *hm = malloc(sizeof(HashMap));
    if(hm == NULL) {return NULL;}
    HashNode **buckets = calloc(DEFAULT_SIZE, sizeof(HashNode*));
    if(buckets == NULL) {
        free(hm);
        return NULL;
    }
    hm->buckets = buckets;
    hm->size = DEFAULT_SIZE;
    hm->item_count = 0;
    hm->get_key = get_key_fn; // Store the translator function
    return hm;
}

HM_RESULT hm_insert(HashMap *hm, void *val) {
    if(update_buffer_size(hm) != HM_OK){   
        return HM_ERR; 
    }
    
    const char *key = hm->get_key(val);
    void *duplicate;
    HM_RESULT status = hm_get(hm, (char*)key, &duplicate);
    if(status == HM_OK){
        return HM_DUPLICATE;
    }
    
    size_t idx = hash_key(key, hm->size);
    HashNode *node = malloc(sizeof(HashNode));
    if(node == NULL){ return HM_OOM; }
    node->val = val; 

    node->next = hm->buckets[idx];
    hm->buckets[idx] = node;
    
    hm->item_count += 1;
    return HM_OK;
}

HM_RESULT hm_get(HashMap *hm, char *key, void **out){
    size_t idx = hash_key(key, hm->size);
    if(hm->buckets[idx] == NULL){
        return HM_NOT_FOUND;
    } else{
        HashNode *current = hm->buckets[idx];
        while(current != NULL){
            if(strcmp(hm->get_key(current->val), key) == 0){
                *out = current->val; // Return the actual item
                return HM_OK;
            }
            current = current->next;
        }
        return HM_NOT_FOUND;
    }
}

HM_RESULT hm_delete(HashMap *hm, char *key, void **out){
    if(update_buffer_size(hm) != HM_OK){   
        return HM_ERR; 
    }
    size_t idx = hash_key(key, hm->size);
    if(hm->buckets[idx] == NULL) { return HM_NOT_FOUND; }
    
    // node at start
    HashNode *current = hm->buckets[idx]->next;
    HashNode *prev = hm->buckets[idx];
    if(strcmp(hm->get_key(prev->val), key) == 0){
        hm->buckets[idx] = prev->next;
        *out = prev->val; 
        free_node(prev); 
        hm->item_count--;
        return HM_OK;
    }
    
    // node in middle/end
    while(current != NULL){
        if(strcmp(hm->get_key(current->val), key) == 0){
            prev->next = current->next;
            // FIXED BUG: Same as above
            *out = current->val; 
            free_node(current);
            hm->item_count--;
            return HM_OK;
        }
        prev = current;
        current = current->next;
    }
    return HM_NOT_FOUND;
}

HM_RESULT hm_free(HashMap *hm){
    return HM_OK;
}

size_t hash_key(const char *key, size_t len) {
    size_t hash = FNV_OFFSET; 
    while (*key) {
        hash ^= (unsigned char)(*key++);
        hash *= FNV_PRIME;
    }
    return hash % len;
}

HM_RESULT update_buffer_size(HashMap *hm){
    if (hm->size == 0) return HM_ERR;
    double load_factor = (double) hm->item_count / hm->size;
    if(load_factor > EXPAND_THRESH) {
        return rehash(hm, hm->size * 2); 
    } else if(load_factor < SHRINK_THRESH){
        if (hm->size <= DEFAULT_SIZE) {
            return HM_OK;
        }
        return rehash(hm, hm->size / 2);
    }
    return HM_OK;
}

HM_RESULT rehash(HashMap *hm, size_t new_size){
    HashNode **old_buckets = hm->buckets;
    size_t old_size = hm->size;
    
    HashNode **new_buckets = calloc(new_size, sizeof(HashNode*));
    if(new_buckets == NULL){ return HM_ERR; }
    
    hm->buckets = new_buckets;
    hm->size = new_size;

    for(size_t i = 0; i < old_size; i++){
        if(old_buckets[i] == NULL){
            continue;
        }

        HashNode *current = old_buckets[i];
        HashNode *next;
        while(current != NULL){
            next = current->next;
            // Dynamically grab the key during rehash
            size_t new_idx = hash_key(hm->get_key(current->val), hm->size);

            current->next = new_buckets[new_idx];
            new_buckets[new_idx] = current;

            current = next;
        }
    }
    free(old_buckets);
    return HM_OK;
}