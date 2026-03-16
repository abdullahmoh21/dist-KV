#include "store/hashmap.h"
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

// Helper prototypes
static size_t _hash_key(char *key, size_t key_len, size_t idx_space_len);
static HM_RESULT _update_buffer_size(HashMap *hm);
static HM_RESULT _rehash(HashMap *hm, size_t new_size);

HashMap* hm_create(KeyView (*get_key_fn)(void *)){
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
    KeyView key_to_add = hm->get_key(val);
    char *key = key_to_add.data;
    size_t key_len = key_to_add.len;
    size_t idx = _hash_key(key, key_len, hm->size);

    HashNode *current = hm->buckets[idx];
    HashNode *prev = NULL;
    while(current != NULL){
        KeyView dup_key = hm->get_key(current->val);
        if(dup_key.len == key_len && memcmp(dup_key.data, key, key_len) == 0){
            return HM_DUPLICATE;
        }
        prev = current;
        current = current->next;
    }

    HashNode *node_to_add = calloc(1, sizeof(HashNode));
    if(node_to_add == NULL){ return HM_OOM; }

    node_to_add->val = val;

    if(prev == NULL){   // bucket was empty
        hm->buckets[idx] = node_to_add;
    } else {
        prev->next = node_to_add;
    }

    hm->item_count++;

    _update_buffer_size(hm);
    return HM_OK;
}

HM_RESULT hm_get(HashMap *hm, char *key, size_t key_len, void **out){
    size_t idx = _hash_key(key, key_len, hm->size);
    if(hm->buckets[idx] == NULL){
        return HM_NOT_FOUND;
    } else{
        HashNode *current = hm->buckets[idx];
        while(current != NULL){
            KeyView kv = hm->get_key(current->val);
            if(kv.len == key_len && memcmp(kv.data, key, kv.len) == 0){
                *out = current->val; 
                return HM_OK;
            }
            current = current->next;
        }
        return HM_NOT_FOUND;
    }
}

HM_RESULT hm_delete(HashMap *hm, char *key, size_t key_len, void **out){
    if(_update_buffer_size(hm) != HM_OK){   
        return HM_ERR; 
    }
    size_t idx = _hash_key(key, key_len, hm->size);
    if(hm->buckets[idx] == NULL) { return HM_NOT_FOUND; }
    
    // node at start
    HashNode *current = hm->buckets[idx]->next;
    HashNode *prev = hm->buckets[idx];
    KeyView kv_p = hm->get_key(prev->val); 
    if(kv_p.len == key_len && memcmp(kv_p.data, key, kv_p.len) == 0){
        hm->buckets[idx] = prev->next;
        *out = prev->val; 
        free(prev); 
        hm->item_count--;
        return HM_OK;
    }
    
    // node in middle/end
    while(current != NULL){
        KeyView kv_c = hm->get_key(current->val);
        if(kv_c.len == key_len && memcmp(kv_c.data, key, kv_c.len) == 0){
            prev->next = current->next;
            *out = current->val; 
            free(current);
            hm->item_count--;
            return HM_OK;
        }
        prev = current;
        current = current->next;
    }
    return HM_NOT_FOUND;
}

HM_RESULT hm_it_init(HashMap *hm, HMIterator *out_it){
    if(hm == NULL || out_it == NULL){
        return HM_ERR;
    }

    out_it->map = hm;
    out_it->bucket_idx = 0;
    out_it->current_node = NULL;

    for(size_t i = 0; i < hm->size; i++){
        if(hm->buckets[i] != NULL){
            out_it->bucket_idx = i;
            out_it->current_node = hm->buckets[i];
            break;
        }
    }

    return HM_OK;
}

HM_RESULT hm_it_next(HMIterator *it, void **out){
    if(it == NULL || it->map == NULL || out == NULL){
        return HM_ERR;
    }

    if(it->current_node == NULL){
        *out = NULL;
        return HM_NOT_FOUND;
    }

    HashMap *map = it->map;
    HashNode *node = it->current_node;
    *out = node->val;

    if(node->next != NULL){
        it->current_node = node->next;
        return HM_OK;
    }

    for(size_t i = it->bucket_idx + 1; i < map->size; i++){
        if(map->buckets[i] != NULL){
            it->bucket_idx = i;
            it->current_node = map->buckets[i];
            return HM_OK;
        }
    }

    it->current_node = NULL;
    return HM_OK;
}

HM_RESULT hm_free_shallow(HashMap *hm){
    for(size_t i = 0; i<hm->size; i++){
        HashNode *current = hm->buckets[i];
        HashNode *next = NULL;
        while(current != NULL){
            next = current->next;
            // simply free our HashNode; leave val for owner 
            free(current);  
            current = next;
        }
    }
    free(hm->buckets);
    free(hm);
    return HM_OK;
}

size_t _hash_key(char *key, size_t key_len, size_t idx_space_len) {
    size_t hash = FNV_OFFSET; 
    for(size_t i = 0; i<key_len; i++) {
        hash ^= (unsigned char)(*key++);
        hash *= FNV_PRIME;
    }
    return hash % idx_space_len;
}

HM_RESULT _update_buffer_size(HashMap *hm){
    if (hm->size == 0) return HM_ERR;
    double load_factor = (double) hm->item_count / hm->size;
    if(load_factor > EXPAND_THRESH) {
        return _rehash(hm, hm->size * 2); 
    } else if(load_factor < SHRINK_THRESH){
        if (hm->size <= DEFAULT_SIZE) {
            return HM_OK;
        }
        return _rehash(hm, hm->size / 2);
    }
    return HM_OK;
}

HM_RESULT _rehash(HashMap *hm, size_t new_size){
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
            // Dynamically grab the key during _rehash
            KeyView kv = hm->get_key(current->val);
            size_t new_idx = _hash_key(kv.data, kv.len, hm->size);

            current->next = new_buckets[new_idx];
            new_buckets[new_idx] = current;

            current = next;
        }
    }
    free(old_buckets);
    return HM_OK;
}