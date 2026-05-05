#include <store/redis_store.h>
#include <store/hashmap.h>
#include <store/skip_list.h>
#include <stdlib.h>
#include <string.h>

// prototypes:
static enum RS_RESULT _add_member(Zset *zset, BulkString *member_str, double score);
static RedisObject*  _create_zset(BulkString *key_str);
static void _free_redis_object(RedisObject *obj);
static void _free_zset(Zset *zset);

KeyView get_redis_object_key(void *obj){
    RedisObject *ro = (RedisObject *) obj;
    return (KeyView){ .data = ro->key, .len = ro->key_len };
}

KeyView get_zset_member(void *obj){
    ZSetMember *zmo = (ZSetMember *) obj;
    return (KeyView){.data = zmo->key, .len = zmo->key_len};
}

enum RS_RESULT create_store(RedisStore *store){
    HashMap *dict = hm_create(get_redis_object_key);
    if(dict == NULL){
    return RS_ERR;
    }
    store->dict = dict;
    store->aof = NULL;
    return RS_OK;
}

enum RS_RESULT rs_flush(RedisStore *store) {
    HMIterator it;
    hm_it_init(store->dict, &it);
    void *obj;
    while (hm_it_next(&it, &obj) == HM_OK) {
        _free_redis_object((RedisObject *)obj);
    }
    KeyView (*get_key_fn)(void *) = store->dict->get_key;
    hm_free_shallow(store->dict);
    store->dict = hm_create(get_key_fn);
    if (store->dict == NULL) return RS_OOM;
    return RS_OK;
}

enum RS_RESULT rs_get(RedisStore *store, BulkString *key_str, RedisObject **out){
    if(key_str == NULL || key_str->data == NULL ){ return RS_BAD_ARG; }
    RedisObject *res;
    size_t hash = hm_compute_hash(key_str->data, key_str->len);
    HM_RESULT search_status = hm_get_h(store->dict, key_str->data, key_str->len, hash, (void*) &res);
    if (search_status == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(search_status != HM_OK) {return RS_ERR; }

    if(res->type != T_KV ){
        return RS_WRONG_TYPE;
    }

    *out = res;
    return RS_OK;
}

enum RS_RESULT rs_set(RedisStore *store, BulkString *key_str, BulkString *data_str){
    if(key_str == NULL || key_str->data == NULL ){ return RS_BAD_ARG; }
    if(data_str == NULL || data_str->data == NULL ){ return RS_BAD_ARG; }

    // Build the new object upfront. hm_find_or_insert_h does one hash + one chain traversal
    // for both the insert and update cases, eliminating the previous double hash+traversal
    // (hm_get to check existence, then hm_insert which hashed and traversed again).
    RedisObject *obj = calloc(1, sizeof(RedisObject));
    if(obj == NULL){ return RS_OOM; }

    obj->key  = malloc(key_str->len);
    obj->data = malloc(data_str->len);
    if(obj->key == NULL || obj->data == NULL){
        free(obj->key);
        free(obj->data);
        free(obj);
        return RS_OOM;
    }

    memcpy(obj->key,  key_str->data,  key_str->len);
    memcpy(obj->data, data_str->data, data_str->len);
    obj->key_len  = key_str->len;
    obj->data_len = data_str->len;
    obj->type     = T_KV;

    // Compute hash from the BulkString (recv buffer) before it's copied into obj.
    // hm_find_or_insert_h reuses this hash — no second FNV loop.
    size_t hash = hm_compute_hash(key_str->data, key_str->len);
    RedisObject *existing = NULL;
    HM_RESULT res = hm_find_or_insert_h(store->dict, obj, hash, (void**)&existing);
    if(res == HM_OOM){
        _free_redis_object(obj);
        return RS_OOM;
    }

    if(existing != NULL){
        // Key already present — type check, then update data in place.
        if(existing->type != T_KV){
            _free_redis_object(obj);
            return RS_WRONG_TYPE;
        }
        void *old_data     = existing->data;
        existing->data     = obj->data;   // transfer ownership of the newly allocated value
        existing->data_len = obj->data_len;
        free(old_data);
        obj->data = NULL;                 // prevent double-free in _free_redis_object
        _free_redis_object(obj);
        return RS_OK;
    }

    // existing == NULL means obj was freshly inserted.
    return RS_OK;
}

enum RS_RESULT rs_delete(RedisStore *store, BulkString *key_str){
    if(key_str == NULL || key_str->data == NULL  ){ return RS_BAD_ARG; }

    RedisObject *obj = NULL;
    size_t hash = hm_compute_hash(key_str->data, key_str->len);
    HM_RESULT del_status = hm_delete_h(store->dict, key_str->data, key_str->len, hash, (void*) &obj);

    if(del_status == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(del_status != HM_OK){ return RS_ERR; }

    _free_redis_object(obj);
    return RS_OK;
}

enum RS_RESULT rs_zadd(RedisStore *store, BulkString *zkey_str, BulkString *member, double score){
    // Compute once — reused for both hm_get_h and, if the zset is new, hm_insert_h.
    size_t zkey_hash = hm_compute_hash(zkey_str->data, zkey_str->len);

    RedisObject *zset_obj = NULL;
    HM_RESULT zset_search = hm_get_h(store->dict, zkey_str->data, zkey_str->len, zkey_hash, (void*) &zset_obj);

    if(zset_search == HM_OK && zset_obj->type != T_ZSET){ return RS_WRONG_TYPE; }
    else if(zset_search == HM_ERR){ return RS_ERR; }
    else if(zset_search == HM_NOT_FOUND){
        zset_obj = _create_zset(zkey_str);
        if(zset_obj == NULL){
            _free_redis_object(zset_obj);
            return RS_ERR;
        }
        // Reuse zkey_hash — same key, same hash, no second FNV loop.
        HM_RESULT zset_added = hm_insert_h(store->dict, zset_obj, zkey_hash);
        if(zset_added != HM_OK){ return RS_ERR; }
    }

    enum RS_RESULT added = _add_member(zset_obj->data, member, score);
    if(!(added == RS_ADDED || added == RS_UPDATED)){
        return RS_ERR;
    }
    return added;
}

enum RS_RESULT rs_zscore(RedisStore *store, BulkString *key_str, BulkString *member_str, double *out){
    RedisObject *zset_obj;
    size_t zkey_hash = hm_compute_hash(key_str->data, key_str->len);
    HM_RESULT zset_search = hm_get_h(store->dict, key_str->data, key_str->len, zkey_hash, (void*) &zset_obj);
    if(zset_search == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(zset_search != HM_OK){ return RS_ERR; }

    if(zset_obj->type != T_ZSET){ return RS_WRONG_TYPE; }

    Zset *zset = (Zset*) zset_obj->data;

    ZSetMember *member;
    size_t member_hash = hm_compute_hash(member_str->data, member_str->len);
    HM_RESULT member_search = hm_get_h(zset->hm, member_str->data, member_str->len, member_hash, (void*) &member);
    if(member_search == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    *out = member->score;
    return RS_OK;
}

enum RS_RESULT rs_get_zset(RedisStore *store, BulkString *key_str, Zset **out){
    RedisObject *zset_obj = NULL;
    size_t hash = hm_compute_hash(key_str->data, key_str->len);
    HM_RESULT zset_search = hm_get_h(store->dict, key_str->data, key_str->len, hash, (void*) &zset_obj);

    if(zset_search == HM_NOT_FOUND || zset_obj == NULL){ return RS_NOT_FOUND; }
    else if(zset_search != HM_OK){ return RS_ERR; }
    if(zset_obj->type != T_ZSET) return RS_WRONG_TYPE;
    *out = (Zset*) zset_obj->data;
    return RS_OK;
}

enum RS_RESULT rs_zset_remove_member(Zset *zset, BulkString *member_str){
    ZSetMember *member;
    size_t hash = hm_compute_hash(member_str->data, member_str->len);
    HM_RESULT h_member_del = hm_delete_h(zset->hm, member_str->data, member_str->len, hash, (void*) &member);

    if(h_member_del == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(h_member_del != HM_OK){ return RS_ERR; }

    SL_RESULT s_member_del = sl_delete(zset->sl, member->key, member->key_len, member->score);
    if(s_member_del == SL_NOT_FOUND){ return RS_NOT_FOUND; }

    free(member->key);
    free(member);

    return RS_OK;
}

// HELPERS

// _add_member: computes the member hash once and reuses it for both the
// existence check (hm_get_h) and the insert (hm_insert_h) — eliminating the
// second FNV loop that hm_insert would have run on new members.
static enum RS_RESULT _add_member(Zset *zset, BulkString *member_str, double score){
    size_t member_hash = hm_compute_hash(member_str->data, member_str->len);

    ZSetMember *existing = NULL;
    HM_RESULT search = hm_get_h(zset->hm, member_str->data, member_str->len, member_hash, (void*) &existing);
    if(search == HM_OK){    // existing member — update score in both structures
        SL_RESULT updated = sl_update(zset->sl, member_str->data, member_str->len, existing->score, score);
        if(updated != SL_OK){
            return updated == SL_NOT_FOUND ? RS_NOT_FOUND : RS_ERR;
        }
        existing->score = score;
        return RS_UPDATED;

    } else if(search == HM_NOT_FOUND){  // new member
        ZSetMember *member_to_add = calloc(1, sizeof(ZSetMember));
        if(member_to_add == NULL){
            return RS_OOM;
        }

        member_to_add->key = malloc(member_str->len);
        if(member_to_add->key == NULL){
            free(member_to_add);
            return RS_OOM;
        }

        memcpy(member_to_add->key, member_str->data, member_str->len);
        member_to_add->key_len = member_str->len;
        member_to_add->score = score;

        // Reuse member_hash — the key bytes are identical (copied from member_str).
        HM_RESULT added_to_hm = hm_insert_h(zset->hm, member_to_add, member_hash);
        if(added_to_hm != HM_OK) {
            free(member_to_add->key);
            free(member_to_add);
            return added_to_hm == HM_OOM ? RS_OOM : RS_ERR;
        }

        SL_RESULT added_to_sl = sl_insert(zset->sl, member_to_add);
        if(added_to_sl != SL_OK){
            ZSetMember *out = NULL;
            hm_delete_h(zset->hm, member_to_add->key, member_to_add->key_len, member_hash, (void*) &out);
            free(member_to_add->key);
            free(member_to_add);
            return added_to_sl == SL_OOM ? RS_OOM : RS_ERR;
        }
        return RS_ADDED;

    } else {    // search ERR
        return RS_ERR;
    }
}

static RedisObject* _create_zset(BulkString *key_str){
    RedisObject *obj = calloc(1, sizeof(RedisObject));
    if (!obj) return NULL;

    Zset *zset = calloc(1, sizeof(Zset));
    if (!zset) {
        free(obj);
        return NULL;
    }

    zset->sl = sl_create(DEFAULT_MAX_LVL);
    zset->hm = hm_create(get_zset_member);

    if (!zset->sl || !zset->hm) {
        if (zset->sl) sl_free_shallow(zset->sl);
        if (zset->hm) hm_free_shallow(zset->hm);
        free(zset);
        free(obj);
        return NULL;
    }

    obj->key = malloc(key_str->len);
    if (!obj->key) {
        sl_free_shallow(zset->sl);
        hm_free_shallow(zset->hm);
        free(zset);
        free(obj);
        return NULL;
    }

    memcpy(obj->key, key_str->data, key_str->len);
    obj->key_len = key_str->len;
    obj->type = T_ZSET;
    obj->data = zset;
    obj->data_len = sizeof(Zset);

    return obj;
}

static void _free_redis_object(RedisObject *obj){
    if(obj == NULL){ return; }

    if(obj->type == T_ZSET){
        _free_zset(obj->data);
    }

    if(obj->key != NULL){
        free(obj->key);
    }

    if(obj->data != NULL){
        free(obj->data);
    }

    free(obj);

    return;
}

static void _free_zset(Zset *zset){
    if(zset == NULL){ return; }

    if(zset->sl != NULL){
        SkipListNode *current = zset->sl->head != NULL ? zset->sl->head->forward[0] : NULL;
        while(current != NULL){
            if(current->obj != NULL){
                free(current->obj->key);
                free(current->obj);
            }
            current = current->forward[0];
        }
        sl_free_shallow(zset->sl);
    }
    if(zset->hm != NULL){
        hm_free_shallow(zset->hm);
    }
}
