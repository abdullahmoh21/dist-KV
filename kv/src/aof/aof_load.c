#include "aof_internal.h"

static void _free_buffer(struct Buffer *b);

enum AOF_RESULT aof_load(RedisStore *store){
    int aof_fd = open("appendonly.aof", O_RDONLY, 0644);
    if (aof_fd == -1) {
        if (errno == ENOENT) {
            // printf("No AOF file found. Starting with a fresh database.\n");
            return AOF_OK;
        } else {
            // perror("Failed to open AOF for recovery");
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
                    // printf("[AOF ERROR] Failed to expand buffer\n");
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
            // perror("Read Error");
            break;
        } else if(n == 0){
            if (buff->read_idx < buff->used) {
                size_t trailing = buff->used - buff->read_idx;
                // fprintf(stderr,
                //     "AOF Error: EOF with %zu unparsed bytes (truncated/corrupt AOF)\n",
                //     trailing);
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
                // fprintf(stderr, "AOF replay failed (%zd): %s\n", consumed, msg);
                _free_buffer(buff);
                close(aof_fd);
                return AOF_PARSE_ERR;
            }
    
            ExecuteResult res = dispatch_command(-1, &command, store);
            if(res != EE_WRITE_OK){
                // printf("A command in AOF failed to execute!\n");
                // fprintf(stderr, "AOF dispatch failed with ExecuteResult=%d\n", res);
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
    // fprintf(stderr, "[AOF LOAD DEBUG] Replay complete. commands=%zu\n", replayed_commands);
    return AOF_OK;
}

static void _free_buffer(struct Buffer *b){
    if(b->data != NULL){
        free(b->data);
    }
    free(b);
}