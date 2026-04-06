#include "aof_internal.h"
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/mman.h>

#define COMPACT_BUF_SIZE (4ULL * 1024 * 1024)

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
void aof_compact(RedisStore *store) {
    // Log initial AOF size
    struct stat aof_stat;
    off_t initial_aof_size = 0;
    if (stat("appendonly.aof", &aof_stat) == 0) {
        initial_aof_size = aof_stat.st_size;
    }
    fprintf(stderr, "[AOF child] starting compaction — initial AOF size: %lld bytes\n",
            (long long)initial_aof_size);

    int aof_fd = open("compacted.aof", O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (aof_fd == -1) {
        perror("[AOF child] open compacted.aof");
        exit(1);
    }

    // mmap instead of malloc — malloc is not safe after fork() in a multithreaded
    // process because the allocator lock may be held by a thread that no longer
    // exists in the child. mmap(MAP_ANON) bypasses the allocator entirely.
    size_t map_size = sizeof(struct Buffer) + COMPACT_BUF_SIZE;
    void *mem = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (mem == MAP_FAILED) {
        perror("[AOF child] mmap");
        close(aof_fd);
        _exit(1);
    }

    struct Buffer *buf = (struct Buffer *)mem;
    buf->data = (char *)mem + sizeof(struct Buffer);
    buf->capacity = COMPACT_BUF_SIZE;
    buf->max_capacity = COMPACT_BUF_SIZE;  // no expansion — flush to disk instead
    buf->used = 0;
    buf->read_idx = 0;

    HMIterator h_it;
    hm_it_init(store->dict, &h_it);

    void *current = NULL;
    int success = 1;
    size_t initial_commands = 0;
    size_t compacted_commands = 0;

    // Count total commands in the initial AOF (one per store entry as a baseline)
    HMIterator count_it;
    hm_it_init(store->dict, &count_it);
    void *tmp = NULL;
    while (hm_it_next(&count_it, &tmp) == HM_OK) initial_commands++;
    fprintf(stderr, "[AOF child] initial AOF: size=%lld bytes, entries=%zu\n",
            (long long)initial_aof_size, initial_commands);

    // walk the store
    while (hm_it_next(&h_it, &current) == HM_OK) {
        RedisObject *obj = (RedisObject*) current;
        enum AOF_RESULT res = AOF_OK;

        if (obj->type == T_KV) {
            res = _write_kv_compaction(aof_fd, buf, obj);
            if (res == AOF_OK) compacted_commands++;
        } else if (obj->type == T_ZSET) {
            res = _write_zset_compaction(aof_fd, buf, obj);
            if (res == AOF_OK) {
                struct Zset *zset = (struct Zset *)obj->data;
                size_t member_count = zset->hm->item_count;
                compacted_commands += (member_count + ZSET_BATCH_SIZE - 1) / ZSET_BATCH_SIZE;
            }
        }

        if (res != AOF_OK) {
            success = 0;
            break;
        }
    }

    if (success) {
        _flush_buffer_to_disk(aof_fd, buf);
        fsync(aof_fd);
        off_t compacted_size = lseek(aof_fd, 0, SEEK_END);
        fprintf(stderr, "[AOF child] compaction done — compacted AOF: size=%lld bytes, commands=%zu\n",
                (long long)compacted_size, compacted_commands);
    } else {
        fprintf(stderr, "[AOF child] compaction failed\n");
    }

    munmap(mem, map_size);
    close(aof_fd);

    _exit(success ? 0 : 1);
}

enum AOF_RESULT aof_merge_compacted(AOFManager *aof){                                                                                                    
  int src = open("tmp.aof", O_RDONLY);             
  if(src == -1){
    return AOF_ERR;
  }        

  int dst = open("compacted.aof", O_WRONLY | O_APPEND);          
  if(dst == -1){
    close(src);
    return AOF_ERR;
  }                                                                            
                                                                                                                                              
  char buf[65536];                                                                                                                            
  ssize_t n;                                                                                                                                  
  while ((n = read(src, buf, sizeof(buf))) > 0) {                                                                                             
      ssize_t written = 0;                                                                                                                    
      while (written < n) {                                                                                                                   
          ssize_t w = write(dst, buf + written, n - written);                                                                                 
          if (w < 0) {
              if (errno == EINTR) continue;
              fsync(dst);
              close(src);
              close(dst);
              return AOF_ERR;
          }
          written += w;                                                                                                                       
      }                                                                                                                                       
  }                                                                                                                                           
  fsync(dst);                                                                                                                                 
  close(src);                                                                                                                                 
  close(dst);
  return AOF_OK;
}

// --- Failure Recovery: append tmp.aof back into appendonly.aof ---
enum AOF_RESULT aof_recover_on_compact_fail(AOFManager *aof) {
    // Flush any commands still buffered in memory so they land in tmp.aof first
    aof_force_flush(aof);

    // Open appendonly.aof for appending (create if it was lost)
    int dst = open("appendonly.aof", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (dst == -1) {
        return AOF_ERR;
    }

    // Copy tmp.aof (writes that accumulated while the child ran) into appendonly.aof
    int src = open("tmp.aof", O_RDONLY);
    if (src != -1) {
        char buf[65536];
        ssize_t n;
        while ((n = read(src, buf, sizeof(buf))) > 0) {
            ssize_t written = 0;
            while (written < n) {
                ssize_t w = write(dst, buf + written, n - written);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    close(src);
                    fsync(dst);
                    close(dst);
                    return AOF_ERR;
                }
                written += w;
            }
        }
        close(src);
        unlink("tmp.aof");
    }

    fsync(dst);

    // Redirect future AOF writes back to appendonly.aof
    int old_fd = aof->fd;
    aof_redirect(aof, dst);
    close(old_fd);

    // Re-sync the in-memory file_size counter with what's actually on disk
    struct stat st;
    if (fstat(dst, &st) == 0) {
        aof->file_size = (uint64_t)st.st_size;
    }

    return AOF_OK;
}

// --- Redirect AOF ---
enum AOF_RESULT aof_redirect(AOFManager *aof, int fd){
    pthread_mutex_lock(&aof->lock);
    aof->fd = fd;
    pthread_mutex_unlock(&aof->lock);
    
    return AOF_OK;
}

enum AOF_RESULT aof_check_compact(AOFManager *aof){
    if(aof->file_size < MIN_AOF_FILE_SIZE || aof->child_pid != -1){
        return AOF_ERR;
    }
    
    if(aof->file_size > aof->last_compaction_file_size * AOF_COMPACTION_FACTOR){
        return AOF_OK;
    }

    return AOF_ERR;
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
    char *end = __write_size_t(buf->data + buf->used, (size_t)num_of_args);
    buf->used = (size_t)(end - buf->data);

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