#include <aof/aof.h>
#include <store/buffer.h>
#include <parser/resp_parser.h>
#include <engine/execution_engine.h>
#include "utils/time.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>

//prototype
static void _force_swap_buffers(AOFManager *aof);
static int _try_swap_buffers(AOFManager *aof);
static void __swap_buffers(AOFManager *aof);
static void _free_buffer(struct Buffer *b);
static int cmd_space_avail(size_t cmd_len, struct Buffer *buff);
static void _buffer_append(struct Buffer *buf, RedisCommand *cmd);
static void* aof_thread(void *arg);

enum AOF_RESULT aof_load(RedisStore *store){
    int aof_fd = open("appendonly.aof", O_RDONLY, 0644);
    if (aof_fd == -1) {
        if (errno == ENOENT) {
            printf("No AOF file found. Starting with a fresh database.\n");
            return AOF_OK;
        } else {
            perror("Failed to open AOF for recovery");
            return AOF_IO_ERR;
        }
    }

    struct Buffer *buff = malloc(sizeof(struct Buffer));
    if(buff == NULL){
        close(aof_fd);
        return AOF_OOM;
    }

    buff->data = malloc(AOF_READ_BUFF_LEN);
    if(buff->data == NULL){
        _free_buffer(buff);
        close(aof_fd);
        return AOF_OOM;
    }

    buff->capacity = AOF_READ_BUFF_LEN;
    buff->max_capacity = AOF_READ_BUFF_LEN_MAX;
    buff->used = 0;
    buff->read_idx = 0;

    size_t replayed_commands = 0;
    
    while(1){
        if(buff->used == buff->capacity){    // buff full and we need more data
            // 1. Try removing used data
            size_t remaining = buff->used - buff->read_idx;
            memmove(buff->data, buff->data + buff->read_idx, remaining);
            buff->read_idx = 0;
            buff->used = remaining;

            // 2. No used data? expand buff
            if(buff->used == buff->capacity){
                if(!expand_buffer(buff)){
                    printf("[AOF ERROR] Failed to expand buffer\n");
                    _free_buffer(buff);
                    close(aof_fd);
                    return AOF_ERR;
                }
            }
        }

        char *buff_end = buff->data + buff->used;

        ssize_t n = read(aof_fd, buff_end, buff->capacity - buff->used);  // append to file
        
        if(n == -1){
            if (errno == EINTR) continue;
            perror("Read Error");
            break;
        } else if(n == 0){
            if (buff->read_idx < buff->used) {
                size_t trailing = buff->used - buff->read_idx;
                fprintf(stderr,
                    "AOF Error: EOF with %zu unparsed bytes (truncated/corrupt AOF)\n",
                    trailing);
                _free_buffer(buff);
                close(aof_fd);
                return AOF_PARSE_ERR;
            }
            break;
        }

        buff->used += n; // partial reads are fine!

        while(buff->read_idx < buff->used){ // while data left in buffer
            struct RedisCommand command;
            char *start_buff = buff->data + buff->read_idx;
            size_t len = buff->used - buff->read_idx;

            ssize_t consumed = parse_array_command(start_buff, len, &command);
    
            if(consumed == 0){
                break;  // pull more data
            } else if(consumed < 0){
                char *msg;
                switch (consumed) {
                    case ERR_INVALID_TYPE:    msg = "AOF Error: expected '*', got other"; break;
                    case ERR_INVALID_ARRAY_L: msg = "AOF Error: invalid multibulk length"; break;
                    case ERR_ARRAY_TOO_BIG:   msg = "AOF Error: too many arguments"; break;
                    case ERR_INVALID_BULK_P:  msg = "AOF Error: expected '$', got other"; break;
                    case ERR_INVALID_DELIM:   msg = "AOF Error: expected CRLF delimiter"; break;
                    case ERR_BULK_TOO_BIG:    msg = "AOF Error: bulk string too long"; break;
                    case ERR_MEM_ALLOC:       msg = "Server error: out of memory"; break;
                    default:                  msg = "AOF Error: unknown error"; break;
                }
                fprintf(stderr, "AOF replay failed (%zd): %s\n", consumed, msg);
                _free_buffer(buff);
                close(aof_fd);
                return AOF_PARSE_ERR;
            }
    
            ExecuteResult res = dispatch_command(-1, &command, store);
            if(res != EE_WRITE_OK){
                printf("A command in AOF failed to execute!\n");
                fprintf(stderr, "AOF dispatch failed with ExecuteResult=%d\n", res);
                _free_buffer(buff);
                close(aof_fd);
                return AOF_EXEC_ERR;
            }

            replayed_commands++;
    
            buff->read_idx += (size_t) consumed;
            free_command(&command);            
        }
    }
    _free_buffer(buff);
    close(aof_fd);
    fprintf(stderr, "[AOF LOAD DEBUG] Replay complete. commands=%zu\n", replayed_commands);
    return AOF_OK;
}

enum AOF_RESULT aof_create(AOFManager **out){
    int aof_fd = open("appendonly.aof", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd == -1) {
        perror("open");
    }

    AOFManager *mgr = malloc(sizeof(AOFManager));
    if(mgr == NULL) return AOF_OOM;

    struct Buffer *active = malloc(sizeof(struct Buffer));
    if(active == NULL){
        free(mgr);
        return AOF_OOM;
    }

    active->data = calloc(1, INITIAL_BUFF_CAPACITY);
    active->capacity = INITIAL_BUFF_CAPACITY;
    active->max_capacity = MAX_AOF_BUF_LIMIT;
    active->read_idx = 0;
    active->used = 0;

    if(active->data == NULL){
        free(mgr);
        free(active);
        close(aof_fd);
        return AOF_OOM;
    }


    struct Buffer *standby = malloc(sizeof(struct Buffer));
    if(standby == NULL){
        free(mgr);
        free(active->data);
        free(active);
        close(aof_fd);
        return AOF_OOM;
    }

    standby->data = calloc(1, INITIAL_BUFF_CAPACITY);
    standby->capacity = INITIAL_BUFF_CAPACITY;
    standby->max_capacity = MAX_AOF_BUF_LIMIT;
    standby->read_idx = 0;
    standby->used = 0;

    if(standby->data == NULL){
        free(mgr);
        free(standby);
        free(active->data);
        free(active);
        close(aof_fd);
        return AOF_OOM;
    }

    mgr->active = active;
    mgr->standby = standby;
    mgr->shutdown = 0;
    mgr->ready_to_flush = 0;
    mgr->write_in_progress = 0;
    mgr->last_flush_ms = monotonic_ms();
    mgr->fd = aof_fd;
    pthread_mutex_init(&mgr->lock, NULL);
    pthread_cond_init(&mgr->cond, NULL);
    if (pthread_create(&mgr->thread_id, NULL, aof_thread, mgr) != 0) {
        perror("pthread_create");
        free(mgr);
        free(standby->data);
        free(standby);
        free(active->data);
        free(active);
        close(aof_fd);
        return AOF_ERR; 
    }
    *out = mgr;
    return AOF_OK;
}

enum AOF_RESULT aof_add(AOFManager *aof, RedisCommand *command) {
    size_t cmd_len = command->raw_len;

    if (cmd_len > aof->active->max_capacity) {
        return AOF_OOM;
    }

    // 2. Attempt: Direct fit
    if (cmd_space_avail(cmd_len, aof->active)) {
        _buffer_append(aof->active, command);
        return AOF_OK;
    }

    // 3. Attempt: Non-blocking swap to standby
    if (_try_swap_buffers(aof)) {
        if (cmd_space_avail(cmd_len, aof->active)) {
            _buffer_append(aof->active, command);
            return AOF_OK;
        }
    }

    // 4. Attempt: Expansion (Up to max_capacity)
    if (aof->active->used + cmd_len <= aof->active->max_capacity) {
        if (expand_buffer_to(aof->active, aof->active->used + cmd_len)) {
            _buffer_append(aof->active, command);
            return AOF_OK;
        }
    }

    // 5. Last Resort: Backpressure Stall
    _force_swap_buffers(aof);

    // 6. Attempt: Might still need to expand new active
    if (!cmd_space_avail(cmd_len, aof->active)) {
        if (!expand_buffer_to(aof->active, cmd_len)) {
            return AOF_OOM; // Should be impossible
        }
    }

    _buffer_append(aof->active, command);
    return AOF_OK;
}

enum AOF_RESULT aof_compact(AOFManager *aof){
    return AOF_OK;
}

void* aof_thread(void *arg) {
    AOFManager *aof = (AOFManager *)arg;
    while(!aof->shutdown){
        pthread_mutex_lock(&aof->lock);

        while(!aof->ready_to_flush){
            pthread_cond_wait(&aof->cond, &aof->lock);
        }
        aof->write_in_progress = 1;
        pthread_mutex_unlock(&aof->lock);


        while(aof->standby->read_idx < aof->standby->used){
            ssize_t n = write(aof->fd,
                aof->standby->data + aof->standby->read_idx,
                aof->standby->used - aof->standby->read_idx);

            if(n == -1){
                if (errno == EINTR) continue;
                perror("AOF Write Error");
                if (errno == ENOSPC) {
                    // critical disk failure
                    exit(1);
                }
                break;
            }
            aof->standby->read_idx += n;
        }
        fdatasync(aof->fd);
        pthread_mutex_lock(&aof->lock);
        aof->standby->read_idx = 0;
        aof->standby->used = 0;
        aof->write_in_progress = 0;
        aof->ready_to_flush = 0;
        pthread_cond_broadcast(&aof->cond);
        pthread_mutex_unlock(&aof->lock);
    }
    return NULL;
}

enum AOF_RESULT aof_check_flush(AOFManager *aof){
    if(aof == NULL) return AOF_ERR;
    if(aof->active->used == 0) return AOF_OK;
    if(monotonic_ms() - aof->last_flush_ms < 1000) return AOF_OK;

    _force_swap_buffers(aof);

    return AOF_OK;
}

void aof_destroy(AOFManager *aof) {
    if (!aof) return;

    pthread_mutex_lock(&aof->lock);
    aof->shutdown = 1; // You'll need to add this int to your struct
    pthread_cond_broadcast(&aof->cond);
    pthread_mutex_unlock(&aof->lock);

    // 2. Wait for the background thread to finish its last write
    pthread_join(aof->thread_id, NULL);

    // 3. Free everything in reverse order of creation
    if (aof->active) {
        free(aof->active->data);
        free(aof->active);
    }
    if (aof->standby) {
        free(aof->standby->data);
        free(aof->standby);
    }
    
    close(aof->fd);
    pthread_mutex_destroy(&aof->lock);
    pthread_cond_destroy(&aof->cond);
    free(aof);
}

// Helpers
static int _try_swap_buffers(AOFManager *aof) {
    if (pthread_mutex_trylock(&aof->lock) != 0) return 0;

    int success = 0;
    // Only swap if the system is actually idle
    if (!aof->ready_to_flush && !aof->write_in_progress) {
        __swap_buffers(aof);
        success = 1;
    }

    pthread_mutex_unlock(&aof->lock);
    return success;
}

static void _force_swap_buffers(AOFManager *aof) {
    pthread_mutex_lock(&aof->lock);
    
    while(aof->ready_to_flush || aof->write_in_progress) {
        pthread_cond_wait(&aof->cond, &aof->lock);
    }
    
    __swap_buffers(aof);
    
    pthread_mutex_unlock(&aof->lock);
}

// Must hold lock, NO WAITING
static void __swap_buffers(AOFManager *aof) {
    struct Buffer *tmp = aof->active;
    aof->active = aof->standby;
    aof->standby = tmp;

    aof->ready_to_flush = 1;
    aof->last_flush_ms = monotonic_ms();
    pthread_cond_signal(&aof->cond);
}


static void _free_buffer(struct Buffer *b){
    if(b->data != NULL){
        free(b->data);
    }
    free(b);
}

static int cmd_space_avail(size_t cmd_len, struct Buffer *buff){
    size_t buf_available = buff->capacity - buff->used;
    if(cmd_len > buf_available){
        return 0;
    }
    return 1;
}

static void _buffer_append(struct Buffer *buf, RedisCommand *cmd) {
    memcpy(buf->data + buf->used, cmd->raw_start, cmd->raw_len);
    buf->used += cmd->raw_len;
}