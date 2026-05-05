#ifndef REDIS_PROTOCOL_H
#define REDIS_PROTOCOL_H

#include <stddef.h>    
#include <sys/types.h>  
#include <store/buffer.h>

// Forward Declaration
struct InputBuffer;

#define MAX_ARGS 1024
#define MAX_BULK_LEN MAX_CLIENT_INPUT_BUFF_LEN
/* Enums and Structs */
typedef enum {
    PARSE_INCOMPLETE    =  0, 
    ERR_INVALID_TYPE    = -1,
    ERR_INVALID_ARRAY_L = -2,
    ERR_ARRAY_TOO_BIG   = -3,
    ERR_INVALID_BULK_P  = -4,
    ERR_INVALID_BULK_L  = -5,
    ERR_BULK_TOO_BIG    = -6,
    ERR_MEM_ALLOC       = -7,
    ERR_INVALID_DELIM   = -8
} ParseResult;

typedef struct BulkString {
    char *data;
    size_t len;
} BulkString;

// Commands with arg_count <= INLINE_ARGS_MAX use stack-allocated inline_args storage
// instead of a heap allocation, eliminating one malloc/free per parsed command.
// INLINE_ARGS_MAX=8 covers every currently implemented command: the largest are
// ZADD (score+member pairs) and ZRANGE with flags (up to 6 args in typical usage).
#define INLINE_ARGS_MAX 8

typedef struct RedisCommand {
    struct BulkString *args;    // points to inline_args or heap, depending on args_on_heap
    int arg_count;
    char *raw_start;
    size_t raw_len;
    int args_on_heap;           // 1 if args was malloc'd (arg_count > INLINE_ARGS_MAX)
    struct BulkString inline_args[INLINE_ARGS_MAX];
} RedisCommand;

/* Function Prototypes */
ssize_t parse_array_command(char *buff, size_t buff_len, struct RedisCommand *out);
void free_command(struct RedisCommand *cmd);
#endif