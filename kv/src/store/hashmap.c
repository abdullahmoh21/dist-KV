#include "store/hashmap.h"
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

// Helper prototypes
static HM_RESULT _update_buffer_size(HashMap *hm);
static HM_RESULT _rehash(HashMap *hm, size_t new_size);
static int _is_power_of_two(size_t n);
static size_t _next_power_of_two(size_t n);

// Recompute the integer resize thresholds after any change to hm->size.
// Avoids floating-point division on every insert/delete.
static void _update_thresholds(HashMap *hm) {
    hm->expand_threshold = (size_t)(hm->size * EXPAND_THRESH);
    hm->shrink_threshold = (size_t)(hm->size * SHRINK_THRESH);
}

// Raw FNV-1a hash — no modulo applied.  Stable across resizes; the bucket
// index is always derived as  hash & (hm->size - 1)  at call time so a
// cached raw hash remains valid after a rehash.
size_t hm_compute_hash(const char *key, size_t key_len) {
    size_t hash = FNV_OFFSET;
    for (size_t i = 0; i < key_len; i++) {
        hash ^= (unsigned char)(*key++);
        hash *= FNV_PRIME;
    }
    return hash;
}

HashMap* hm_create(KeyView (*get_key_fn)(void *)){
    size_t initial_size = _next_power_of_two(DEFAULT_SIZE);
    HashMap *hm = malloc(sizeof(HashMap));
    if(hm == NULL) {return NULL;}
    HashNode **buckets = calloc(initial_size, sizeof(HashNode*));
    if(buckets == NULL) {
        free(hm);
        return NULL;
    }
    hm->buckets = buckets;
    hm->size = initial_size;
    hm->item_count = 0;
    hm->resize_paused = 0;
    hm->get_key = get_key_fn;
    _update_thresholds(hm);
    return hm;
}

// ── _h variants: caller supplies the raw FNV hash, we apply the modulo. ──────

HM_RESULT hm_get_h(HashMap *hm, const char *key, size_t key_len, size_t hash, void **out) {
    size_t idx = hash & (hm->size - 1);
    HashNode *current = hm->buckets[idx];
    while (current != NULL) {
        KeyView kv = hm->get_key(current->val);
        if (kv.len == key_len && memcmp(kv.data, key, key_len) == 0) {
            *out = current->val;
            return HM_OK;
        }
        current = current->next;
    }
    return HM_NOT_FOUND;
}

HM_RESULT hm_insert_h(HashMap *hm, void *val, size_t hash) {
    KeyView key_to_add = hm->get_key(val);
    size_t idx = hash & (hm->size - 1);

    HashNode *current = hm->buckets[idx];
    HashNode *prev = NULL;
    while (current != NULL) {
        KeyView dup_key = hm->get_key(current->val);
        if (dup_key.len == key_to_add.len &&
            memcmp(dup_key.data, key_to_add.data, key_to_add.len) == 0) {
            return HM_DUPLICATE;
        }
        prev = current;
        current = current->next;
    }

    // malloc instead of calloc: both fields (val, next) are written immediately
    // below, so zero-initialising the 16-byte struct is wasted work on every insert.
    HashNode *node = malloc(sizeof(HashNode));
    if (!node) return HM_OOM;
    node->val  = val;
    node->next = NULL;

    if (prev == NULL) hm->buckets[idx] = node;
    else              prev->next = node;

    hm->item_count++;
    _update_buffer_size(hm);
    return HM_OK;
}

HM_RESULT hm_delete_h(HashMap *hm, const char *key, size_t key_len, size_t hash, void **out) {
    // _update_buffer_size may rehash and change hm->size; reapply the modulo
    // after the call so the bucket index reflects the current table layout.
    if (_update_buffer_size(hm) != HM_OK) return HM_ERR;
    size_t idx = hash & (hm->size - 1);
    if (hm->buckets[idx] == NULL) return HM_NOT_FOUND;

    // node at start
    HashNode *prev    = hm->buckets[idx];
    HashNode *current = prev->next;
    KeyView kv_p = hm->get_key(prev->val);
    if (kv_p.len == key_len && memcmp(kv_p.data, key, key_len) == 0) {
        hm->buckets[idx] = prev->next;
        *out = prev->val;
        free(prev);
        hm->item_count--;
        return HM_OK;
    }

    // node in middle/end
    while (current != NULL) {
        KeyView kv_c = hm->get_key(current->val);
        if (kv_c.len == key_len && memcmp(kv_c.data, key, key_len) == 0) {
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

HM_RESULT hm_find_or_insert_h(HashMap *hm, void *new_val, size_t hash, void **existing_out) {
    KeyView new_kv = hm->get_key(new_val);
    size_t idx = hash & (hm->size - 1);

    HashNode *current = hm->buckets[idx];
    HashNode *prev    = NULL;

    while (current != NULL) {
        KeyView kv = hm->get_key(current->val);
        if (kv.len == new_kv.len && memcmp(kv.data, new_kv.data, new_kv.len) == 0) {
            *existing_out = current->val;
            return HM_OK;
        }
        prev    = current;
        current = current->next;
    }

    HashNode *node = malloc(sizeof(HashNode));
    if (!node) return HM_OOM;
    node->val  = new_val;
    node->next = NULL;

    if (prev == NULL) hm->buckets[idx] = node;
    else              prev->next = node;

    hm->item_count++;
    *existing_out = NULL;
    _update_buffer_size(hm);
    return HM_OK;
}

// ── Public wrappers: compute hash then delegate to the _h variant. ────────────

HM_RESULT hm_insert(HashMap *hm, void *val) {
    KeyView kv = hm->get_key(val);
    return hm_insert_h(hm, val, hm_compute_hash(kv.data, kv.len));
}

HM_RESULT hm_get(HashMap *hm, char *key, size_t key_len, void **out) {
    return hm_get_h(hm, key, key_len, hm_compute_hash(key, key_len), out);
}

HM_RESULT hm_delete(HashMap *hm, char *key, size_t key_len, void **out) {
    return hm_delete_h(hm, key, key_len, hm_compute_hash(key, key_len), out);
}

HM_RESULT hm_find_or_insert(HashMap *hm, void *new_val, void **existing_out) {
    KeyView kv = hm->get_key(new_val);
    return hm_find_or_insert_h(hm, new_val, hm_compute_hash(kv.data, kv.len), existing_out);
}

// ── Iterator ──────────────────────────────────────────────────────────────────

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

// ── Internal resize logic ─────────────────────────────────────────────────────

HM_RESULT _update_buffer_size(HashMap *hm){
    if (hm->size == 0) return HM_ERR;
    if (hm->resize_paused) return HM_OK;

    size_t min_size = _next_power_of_two(DEFAULT_SIZE);

    if(!_is_power_of_two(hm->size)) {
        size_t normalized_size = _next_power_of_two(hm->size);
        if(normalized_size < min_size) {
            normalized_size = min_size;
        }
        return _rehash(hm, normalized_size);
    }

    // Integer comparisons against precomputed thresholds — no float division per call.
    if(hm->item_count > hm->expand_threshold) {
        return _rehash(hm, hm->size << 1);
    } else if(hm->item_count < hm->shrink_threshold){
        if (hm->size <= min_size) {
            return HM_OK;
        }
        size_t target_size = hm->size >> 1;
        if(target_size < min_size) {
            target_size = min_size;
        }
        return _rehash(hm, target_size);
    }
    return HM_OK;
}

HM_RESULT _rehash(HashMap *hm, size_t new_size){
    size_t min_size = _next_power_of_two(DEFAULT_SIZE);
    if(new_size < min_size) {
        new_size = min_size;
    }

    if(!_is_power_of_two(new_size)) {
        new_size = _next_power_of_two(new_size);
    }

    if(new_size == hm->size) {
        return HM_OK;
    }

    HashNode **old_buckets = hm->buckets;
    size_t old_size = hm->size;

    HashNode **new_buckets = calloc(new_size, sizeof(HashNode*));
    if(new_buckets == NULL){ return HM_ERR; }

    hm->buckets = new_buckets;
    hm->size = new_size;
    _update_thresholds(hm);

    for(size_t i = 0; i < old_size; i++){
        if(old_buckets[i] == NULL){
            continue;
        }

        HashNode *current = old_buckets[i];
        HashNode *next;
        while(current != NULL){
            next = current->next;
            KeyView kv = hm->get_key(current->val);
            // Recompute the bucket index using the new table size.
            // hm_compute_hash is used here to separate FNV from the modulo step.
            size_t new_idx = hm_compute_hash(kv.data, kv.len) & (new_size - 1);

            current->next = new_buckets[new_idx];
            new_buckets[new_idx] = current;

            current = next;
        }
    }
    if (!hm->resize_paused) {
        free(old_buckets);
    }
    return HM_OK;
}

static int _is_power_of_two(size_t n) {
    return n != 0 && (n & (n - 1)) == 0;
}

static size_t _next_power_of_two(size_t n) {
    if(n <= 1) {
        return 1;
    }

    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if(sizeof(size_t) > 4) {
        n |= n >> 32;
    }
    n++;

    return n;
}

void hm_pause_resize(HashMap *hm) {
    if (hm) hm->resize_paused = 1;
}

void hm_resume_resize(HashMap *hm) {
    if (hm) hm->resize_paused = 0;
}
