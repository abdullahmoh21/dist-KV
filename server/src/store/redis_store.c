#include <store/redis_store.h>

char* get_redis_object_key(void *obj){
    RedisObject *redis_obj = (RedisObject *) obj;
    return redis_obj->key;
}

char* get_zset_member(void *obj){
    ZSetMember *zmember_obj = (ZSetMember *) obj;
    return zmember_obj->member;
}

enum RS_STATUS create_store(RedisStore *store){
    HashMap *dict = hm_create(get_redis_object_key);
    if(dict == NULL){
    return RS_ERR;
    }
    store->dict = dict;
    return RS_OK;
}

enum RS_STATUS rs_get(RedisStore *store, char *key, RedisObject **out){
    RedisObject *res;
    HM_RESULT search_status = hm_get(store->dict, key, &res);
    if (search_status == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(search_status != HM_OK) {return RS_ERR; }

    if(res->type != T_KV ){
        return RS_WRONG_TYPE;
    } 

    *out = res;
    return RS_OK;
}

enum RS_STATUS rs_put(RedisStore *store, BulkString *key_str, BulkString *data_str){
    char *nulled_key = strndup(key_str->data, key_str->len);
    if(!nulled_key) { 
        return RS_OOM; 
    }
    RedisObject *dup_bj;
    HM_RESULT search_status = hm_get(store->dict, nulled_key, &dup_bj);
    if(search_status == HM_OK){
        free(nulled_key);
        return RS_DUPLICATE; 
    } else if(search_status != HM_NOT_FOUND && search_status != HM_OK) {
        free(nulled_key);
        return RS_ERR;
    }

    RedisObject *obj = calloc(1, sizeof(RedisObject));
    if(obj == NULL){
        return RS_OOM;
    }

    obj->key = nulled_key;
    obj->type = T_KV;
    obj->data_len = data_str->len;

    obj->data = malloc(data_str->len);
    if(obj->data == NULL) {
        free_redis_object(obj); // will handle partial obj's
        return RS_OOM;
    } 

    memcpy(obj->data, data_str->data, data_str->len);

    HM_RESULT insert_status = hm_insert(store->dict, obj);
    if(insert_status != HM_OK){ 
        free_redis_object(obj);
        return insert_status == HM_OOM ? RS_OOM : RS_ERR;
    }
    return RS_OK;
}

enum RS_STATUS rs_delete(RedisStore *store, BulkString *key_str){
    if(key_str == NULL || key_str->data == NULL){ return RS_BAD_ARG; }

    char *nulled_key = malloc(key_str->len + 1);
    if(nulled_key == NULL) { return RS_OOM; }
    memcpy(nulled_key, key_str->data, key_str->len);
    nulled_key[key_str->len] = '\0';

    RedisObject *obj = NULL;
    HM_RESULT del_status = hm_delete(store->dict, nulled_key, &obj);
    free(nulled_key); 
    
    if(del_status == HM_NOT_FOUND){ return RS_NOT_FOUND; }
    else if(del_status != HM_OK){ return RS_ERR; }
    
    free_redis_object(obj);
}

enum RS_STATUS rs_zadd(RedisStore *store, BulkString *zkey_str, BulkString *member, double score){
    char *nulled_zkey = strndup(zkey_str->data, zkey_str->len);
    if(!nulled_zkey == NULL){ return RS_OOM; }

    RedisObject *zset_obj = NULL;
    HM_RESULT zset_search = hm_get(store->dict, nulled_zkey, &zset_obj);

    if(zset_search == HM_OK && zset_obj->type != T_ZSET){
        free(nulled_zkey);
        return RS_WRONG_TYPE;
    } else if(zset_search == HM_ERR){
        free(nulled_zkey);
        return RS_ERR;
    } else if(zset_search == HM_NOT_FOUND){
        zset_obj = create_zset();
        if(zset_obj == NULL){
            free(nulled_zkey);
            return RS_OOM;
        } 
        hm_insert(store->dict, zset_obj);
    } 

    char *nulled_member = strndup(member->data, member->len);
    if(!nulled_member == NULL){ 
        free(nulled_zkey);
        return RS_OOM; 
    }

    ZSetMember *member_to_add = malloc(sizeof(ZSetMember));
    if(!member){ return RS_OOM; }

    member_to_add->member = nulled_member;
    member_to_add->score = score;

    if(add_member(zset_obj, member) != RS_OK){
        free(nulled_zkey);
        free(null);
        return RS_ERR;
    }
}

enum RS_STATUS rs_zget(RedisStore *store, BulkString *key_str){}

enum RS_STATUS rs_zrange(RedisStore *store, BulkString *key_str){}


// HELPERS
void free_redis_object(RedisObject *obj){
    if(obj == NULL){ return; }

    if(obj->key != NULL){
        free(obj->key);
    }

    if(obj->type = T_ZSET){
        free_zset(obj);
    }

    if(obj->data != NULL){
        free(obj->data);
    }
    

    free(obj);

    return;
}

void free_zset(RedisObject *obj){
    if(obj->data != NULL){
        Zset *zset = (Zset*) obj;
        if(zset->sl != NULL){
            sl_free(zset->sl);  
        }
        if(zset->hm != NULL){
            hm_free(zset->hm);  // should it free RedisObjects that are owned by store?
        }
    }
}