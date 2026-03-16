#include "aof_internal.h"
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

// --- Forward Declarations ---
static enum AOF_RESULT _buffer_ensure_space(int fd, struct Buffer *buf, size_t needed);
static enum AOF_RESULT _write_kv_compaction(int aof_fd, struct Buffer *buf, RedisObject *obj);
static enum AOF_RESULT _append_set_buf(struct Buffer *buf, RedisObject *obj);
static enum AOF_RESULT _write_zset_compaction(int aof_fd, struct Buffer *buf, RedisObject *obj);
static enum AOF_RESULT _ensure_and_append_zadd(int aof_fd, struct Buffer *buf, const char *key, size_t key_len, struct ZSetMember **members, int count, size_t batch_len);
static enum AOF_RESULT _append_zadd_buf(struct Buffer *buf, const char *key, size_t key_len, struct ZSetMember **members, int count);
static enum AOF_RESULT _flush_buffer_to_disk(int fd, struct Buffer *buf);

// --- Centralized Buffer Management ---
static enum AOF_RESULT _buffer_ensure_space(int fd, struct Buffer *buf, size_t needed) {
    if (needed > buf->max_capacity) return AOF_OOM; 

    if (buf->used + needed > buf->capacity) {
        if (_flush_buffer_to_disk(fd, buf) != AOF_OK) return AOF_ERR;
        
        // If one massive key/value needs more space than our current capacity even after a flush
        if (needed > buf->capacity) {
            size_t new_size = needed > (buf->capacity * 2) ? needed : (buf->capacity * 2);
            if (new_size > buf->max_capacity) return AOF_OOM;
            if (expand_buffer_to(buf, new_size) != AOF_OK) return AOF_OOM;
        }
    }
    return AOF_OK;
}

// --- Compaction Entry Point ---
// will be forked 
void aof_compact(RedisStore *store) {
    int aof_fd = open("compacted.temp", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd == -1) {
        // perror("open");
        exit(1);
    }

    struct Buffer *buf = malloc(sizeof(struct Buffer));
    if (!buf) {
        close(aof_fd);
        exit(1);
    }

    buf->data = calloc(1, CLIENT_INPUT_BUFF_LEN);
    if (!buf->data) {
        free(buf);
        close(aof_fd);
        exit(1);
    }

    buf->capacity = CLIENT_INPUT_BUFF_LEN;
    buf->max_capacity = MAX_CLIENT_INPUT_BUFF_LEN;
    buf->used = 0;
    buf->read_idx = 0;

    HMIterator h_it;
    hm_it_init(store->dict, &h_it);
    
    void *current = NULL;
    int success = 1;

    // walk the store
    while (hm_it_next(&h_it, &current) == HM_OK) {
        RedisObject *obj = (RedisObject*) current;
        enum AOF_RESULT res = AOF_OK;

        if (obj->type == T_KV) {
            res = _write_kv_compaction(aof_fd, buf, obj);
        } else if (obj->type == T_ZSET) {
            res = _write_zset_compaction(aof_fd, buf, obj);
        }

        if (res != AOF_OK) {
            success = 0;
            break; 
        }
    }

    if (success) {
        _flush_buffer_to_disk(aof_fd, buf);
        fsync(aof_fd); 
    }

    free(buf->data);
    free(buf);
    close(aof_fd);
    
    exit(success ? 0 : 1);
}

// --- KV Compaction ---
static enum AOF_RESULT _write_kv_compaction(int aof_fd, struct Buffer *buf, RedisObject *obj) {
    size_t bytes_needed = 64 + obj->key_len + obj->data_len + _digits(obj->key_len) + _digits(obj->data_len);

    enum AOF_RESULT space_res = _buffer_ensure_space(aof_fd, buf, bytes_needed);
    if (space_res != AOF_OK) return space_res;

    return _append_set_buf(buf, obj);
}

static enum AOF_RESULT _append_set_buf(struct Buffer *buf, RedisObject *obj) {
    // 1. Array Header (Fixed size)
    memcpy(buf->data + buf->used, SET_HEADER, SET_HEADER_LEN);
    buf->used += SET_HEADER_LEN;
    
    // 2. Write Key
    _append_len(buf, obj->key_len);
    memcpy(buf->data + buf->used, obj->key, obj->key_len);
    buf->used += obj->key_len; 
    buf->data[buf->used++] = '\r';
    buf->data[buf->used++] = '\n';

    // 3. Write Data
    _append_len(buf, obj->data_len);
    memcpy(buf->data + buf->used, obj->data, obj->data_len);
    buf->used += obj->data_len; 
    buf->data[buf->used++] = '\r';
    buf->data[buf->used++] = '\n';

    return AOF_OK;
}

// --- ZSET Compaction ---
static enum AOF_RESULT _write_zset_compaction(int aof_fd, struct Buffer *buf, RedisObject *obj) {
    if (obj->type != T_ZSET) return AOF_ERR;
    
    struct Zset *zset = (Zset*)obj->data;
    HMIterator h_it;
    hm_it_init(zset->hm, &h_it);
    
    struct ZSetMember *members[ZSET_BATCH_SIZE] = { NULL };
    void *current = NULL;
    int i = 0;
    
    size_t current_batch_bytes = 64 + obj->key_len + _digits(obj->key_len); 
    size_t flush_threshold = (buf->max_capacity * 4) / 5; 

    while (hm_it_next(&h_it, &current) == HM_OK) { 
        ZSetMember *mbr = (ZSetMember*) current;
        members[i++] = mbr;
        current_batch_bytes += mbr->key_len + 64; 

        if (i == ZSET_BATCH_SIZE || current_batch_bytes > flush_threshold) {
            enum AOF_RESULT status = _ensure_and_append_zadd(aof_fd, buf, obj->key, obj->key_len, members, i, current_batch_bytes);
            if (status != AOF_OK) return status;

            i = 0;
            current_batch_bytes = 64 + obj->key_len + _digits(obj->key_len);
        }
    }

    if (i > 0) {
        return _ensure_and_append_zadd(aof_fd, buf, obj->key, obj->key_len, members, i, current_batch_bytes);
    }

    return AOF_OK;
} 

static enum AOF_RESULT _ensure_and_append_zadd(int aof_fd, struct Buffer *buf, const char *key, size_t key_len, struct ZSetMember **members, int count, size_t batch_len) {
    enum AOF_RESULT space_res = _buffer_ensure_space(aof_fd, buf, batch_len);
    if (space_res != AOF_OK) return space_res;

    return _append_zadd_buf(buf, key, key_len, members, count);
}

static enum AOF_RESULT _append_zadd_buf(struct Buffer *buf, const char *key, size_t key_len, struct ZSetMember **members, int count) {
    // *<num_args>\r\n
    buf->data[buf->used++] = '*';

    int num_of_args = (2*count) + 2;    // ZADD + key + members + doubles
    char *end_of_window = buf->data + buf->used + 20; 
    char *start = itoa((uint64_t)num_of_args, end_of_window);
    
    size_t digit_count = (size_t)(end_of_window - start);
    
    memmove(buf->data + buf->used, start, digit_count);
    buf->used += digit_count;

    buf->data[buf->used++] = '\r';
    buf->data[buf->used++] = '\n';

    // $4\r\nZADD\r\n
    _append_len(buf, 4);
    memcpy(buf->data + buf->used, "ZADD", 4);
    buf->used += 4;
    buf->data[buf->used++] = '\r';
    buf->data[buf->used++] = '\n';
    
    // $<key_len>\r\n<key>\r\n
    _append_len(buf, key_len);
    memcpy(buf->data + buf->used, key, key_len);
    buf->used += key_len;
    buf->data[buf->used++] = '\r';
    buf->data[buf->used++] = '\n';

    for(int i = 0; i < count; i++){
        ZSetMember *mbr = members[i];
     
        // $<double_str_len>\r\n<double_str>\r\n
        char double_str[64];
        ssize_t double_len = sprintf(double_str, "%.17g", mbr->score);
        if(double_len < 0){
            return AOF_ERR;
        }
        _append_len(buf, (size_t) double_len);
        memcpy(buf->data + buf->used, double_str, double_len);
        buf->used += double_len;
        buf->data[buf->used++] = '\r';
        buf->data[buf->used++] = '\n';

        // $<member_len>\r\n<member>\r\n
        _append_len(buf, mbr->key_len);
        memcpy(buf->data + buf->used, mbr->key, mbr->key_len);
        buf->used += mbr->key_len;
        buf->data[buf->used++] = '\r';
        buf->data[buf->used++] = '\n';
    }
    return AOF_OK;
}

// --- I/O Flush ---
static enum AOF_RESULT _flush_buffer_to_disk(int fd, struct Buffer *buf) {
    size_t written = 0;
    while (written < buf->used) {
        ssize_t n = write(fd, buf->data + written, buf->used - written);
        
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return AOF_ERR; 
        }
        written += n;
    }
    
    buf->used = 0; 
    return AOF_OK;
}