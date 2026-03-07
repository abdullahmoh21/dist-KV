#include <store/redis_store.h>
#include <stdlib.h>
#include <string.h>

// prototypes:
static enum RS_RESULT _add_member(Zset *zset, BulkString *member_str, double score);
static RedisObject*  _create_zset(BulkString *key_str);
static void _free_redis_object(RedisObject *obj);
static void _free_zset(Zset *zset);

const KeyView get_redis_object_key(void *obj){
    RedisObject *ro = (RedisObject *) obj;
    return (KeyView){ .data = ro->key, .len = ro->key_len };
}

const KeyView get_zset_member(void *obj){
    ZSetMember *zmo = (ZSetMember *) obj;
    return (KeyView){.data = zmo->key, .len = zmo->key_len};
}

enum RS_RESULT create_store(RedisStore *store){
    HashMap *dict = hm_create(get_redis_object_key);
    if(dict == NULL){
    return RS_ERR;
    }
    store->dict = dict;
    return RS_OK;
}

enum RS_RESULT rs_get(RedisStore *store, BulkString *key_str, RedisObject **out){
    if(key_str == NULL || key_str->data == NULL ){ return RS_BAD_ARG; }
    RedisObject *res;
    HM_RESULT search_status = hm_get(store->dict, key_str->data, key_str->len, (void*) &res);
    if (search_status == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(search_status != HM_OK) {return RS_ERR; }
    
    if(res->type != T_KV ){
        return RS_WRONG_TYPE;
    } 

    *out = res;
    return RS_OK;
}

enum RS_RESULT rs_set(RedisStore *store, BulkString *key_str, BulkString *data_str){
    // yap: currently we do two traversals of the HM, one to find duplicates, and one to actually insert. this is negligible for a bucket 2/3 nodes deep. 
    // however we also hash the key twice maybe add an optional hash parameter to hm_* functions? one could also implement a double pointer solution to find slot and then add... mabye later.
    
    if(key_str == NULL || key_str->data == NULL ){ return RS_BAD_ARG; }
    if(data_str == NULL || data_str->data == NULL ){ return RS_BAD_ARG; }

    
    RedisObject *existing;
    HM_RESULT search_status = hm_get(store->dict, key_str->data, key_str->len, (void*) &existing);

    if(search_status == HM_OK && existing->type != T_KV){ return RS_WRONG_TYPE; }

    else if(search_status == HM_OK){
        void* new_data = malloc(data_str->len);
        if(new_data == NULL) { return RS_OOM; }
        free(existing->data);
        memcpy(new_data, data_str->data, data_str->len);
        existing->data = new_data;
        existing->data_len = data_str->len;
        return RS_OK;
    } 

    RedisObject *obj = calloc(1, sizeof(RedisObject));
    if(obj == NULL){ 
        return RS_OOM; 
    }

    obj->key = malloc(key_str->len);
    obj->data = malloc(data_str->len);

    if (obj->key == NULL || obj->data == NULL) {
            free(obj->key);
            free(obj->data);
            free(obj);
            return RS_OOM;
    }

    memcpy(obj->key, key_str->data, key_str->len);
    memcpy(obj->data, data_str->data, data_str->len);
    obj->key_len = key_str->len;
    obj->data_len = data_str->len;
    obj->type = T_KV;


    HM_RESULT insert_status = hm_insert(store->dict, obj);
    if(insert_status != HM_OK){ 
        _free_redis_object(obj);
        return insert_status == HM_OOM ? RS_OOM : RS_ERR;
    }
    return RS_OK;
}

enum RS_RESULT rs_delete(RedisStore *store, BulkString *key_str){
    if(key_str == NULL || key_str->data == NULL  ){ return RS_BAD_ARG; }

    RedisObject *obj = NULL;
    HM_RESULT del_status = hm_delete(store->dict, key_str->data, key_str->len, (void*) &obj);
    
    if(del_status == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(del_status != HM_OK){ return RS_ERR; }
    
    _free_redis_object(obj);
    return RS_OK;
}

enum RS_RESULT rs_zadd(RedisStore *store, BulkString *zkey_str, BulkString *member, double score){
    RedisObject *zset_obj = NULL;
    HM_RESULT zset_search = hm_get(store->dict, zkey_str->data, zkey_str->len, (void*) &zset_obj);

    if(zset_search == HM_OK && zset_obj->type != T_ZSET){ return RS_WRONG_TYPE; }
    else if(zset_search == HM_ERR){ return RS_ERR; } 
    else if(zset_search == HM_NOT_FOUND){
        zset_obj = _create_zset(zkey_str);
        if(zset_obj == NULL){
            _free_redis_object(zset_obj);
            return RS_ERR;
        } 
        HM_RESULT zset_added = hm_insert(store->dict, zset_obj);
        if(zset_added != HM_OK){ return RS_ERR; }   // should we delete zset?
    } 

    enum RS_RESULT added = _add_member(zset_obj->data, member, score);
    if(!(added == RS_ADDED || added == RS_UPDATED)){      // this will atomically add to both hashmap and skiplist
        return RS_ERR;
    }
    return added;
}

enum RS_RESULT rs_zscore(RedisStore *store, BulkString *key_str, BulkString *member_str, double *out){
    RedisObject *zset_obj;
    HM_RESULT zset_search = hm_get(store->dict, key_str->data, key_str->len, (void*) &zset_obj);
    if(zset_search == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(zset_search != HM_OK){ return RS_ERR; }
    
    if(zset_obj->type != T_ZSET){ return RS_WRONG_TYPE; }

    Zset *zset = (Zset*) zset_obj->data;

    ZSetMember *member;
    HM_RESULT member_search = hm_get(zset->hm, member_str->data, member_str->len, (void*) &member);
    if(member_search == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    *out = member->score;
    return RS_OK;
}

enum RS_RESULT rs_get_zset(RedisStore *store, BulkString *key_str, Zset **out){
    RedisObject *zset_obj = NULL;
    HM_RESULT zset_search = hm_get(store->dict, key_str->data, key_str->len, (void*) &zset_obj);

    if(zset_search == HM_NOT_FOUND || zset_obj == NULL){ return RS_NOT_FOUND; }
    else if(zset_search != HM_OK){ return RS_ERR; }
    if(zset_obj->type != T_ZSET) return RS_WRONG_TYPE;
    *out = (Zset*) zset_obj->data;
    return RS_OK;
}

enum RS_RESULT rs_zset_remove_member(Zset *zset, BulkString *member_str){
    ZSetMember *member;
    HM_RESULT h_member_del = hm_delete(zset->hm, member_str->data, member_str->len, (void*) &member);

    if(h_member_del == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(h_member_del != HM_OK){ return RS_ERR; }

    SL_RESULT s_member_del = sl_delete(zset->sl, member->key, member->key_len, member->score);
    if(s_member_del == SL_NOT_FOUND){ return RS_NOT_FOUND; }    

    free(member->key);
    free(member);

    return RS_OK;
}

enum RS_RESULT rs_zrange_score_init(RedisStore *store, BulkString *key, double min, double max, RS_ZIterator *out_it) {
    if(store == NULL || key == NULL ) return RS_ERR;

    RedisObject *obj = NULL;
    HM_RESULT search = hm_get(store->dict, key->data, key->len, (void*) &obj);

    if(search == HM_NOT_FOUND) return RS_NOT_FOUND;
    if(search != HM_OK) return RS_ERR;

    if(obj->type != T_ZSET) return RS_WRONG_TYPE;

    Zset *zs = (Zset*) obj->data;
    out_it->internal_it = sl_iterator_score(zs->sl, min, max);
    return RS_OK;
}

ZSetMember* rs_ziterator_next(RS_ZIterator *it){
    if (sl_next(&it->internal_it)) {
        return it->internal_it.current->obj;
    }
    return NULL;
}

// HELPERS
static enum RS_RESULT _add_member(Zset *zset, BulkString *member_str, double score){
    ZSetMember *existing = NULL;
    HM_RESULT search = hm_get(zset->hm, member_str->data, member_str->len, (void*) &existing);
    if(search == HM_OK){    // existing member
        SL_RESULT updated = sl_update(zset->sl, member_str->data, member_str->len, existing->score, score);
        if(updated != SL_OK){
            return updated == SL_NOT_FOUND ? RS_NOT_FOUND : RS_ERR;
        }
        //update hashmap
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

        HM_RESULT added_to_hm = hm_insert(zset->hm, member_to_add);
        if(added_to_hm != HM_OK) {
            free(member_to_add->key);
            free(member_to_add);
            return added_to_hm == HM_OOM ? RS_OOM : RS_ERR;
        }

        SL_RESULT added_to_sl = sl_insert(zset->sl, member_to_add);
        if(added_to_sl != SL_OK){
            ZSetMember *out = NULL; 
            hm_delete(zset->hm, member_to_add->key, member_to_add->key_len, (void*) &out);
            free(member_to_add->key);
            free(member_to_add);
            return added_to_sl == SL_OOM ? RS_OOM : RS_ERR;
        }
        return RS_ADDED;
        
    } else {    // seach ERR
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
        SkipListIterator it = sl_iterator_rank(zset->sl, 0, -1);
        while(sl_next(&it)){
            if(it.current != NULL){
                free(it.current->obj);
            }
        }
        sl_free_shallow(zset->sl);  
    }
    if(zset->hm != NULL){
        hm_free_shallow(zset->hm);  
    }
}