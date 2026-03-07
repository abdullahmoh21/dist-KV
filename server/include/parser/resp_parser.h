#ifndef REDIS_PROTOCOL_H
#define REDIS_PROTOCOL_H

#include <stddef.h>    
#include <sys/types.h>  

// Forward Declaration
struct InputBuffer;

#define MAX_ARGS 1024
#define MAX_BULK_LEN 512 * 1024
/* Enums and Structs */
typedef enum {
    PARSE_INCOMPLETE    =  0, 
    ERR_INVALID_TYPE    = -1,
    ERR_INVALID_ARRAY_L = -2,
    ERR_ARRAY_TOO_BIG   = -3,
    ERR_INVALID_BULK_P  = -4,
    ERR_INVALID_BULK_L  = -5,
    ERR_BULK_TOO_BIG    = -6,
    ERR_MEM_ALLOC       = -7
} ParseResult;

typedef struct BulkString {
    char *data;
    size_t len;
} BulkString;

typedef struct RedisCommand {
    struct BulkString *args;
    int arg_count;
} RedisCommand;

/* Function Prototypes */
ssize_t parse_array_command(char *buff, size_t buff_len, struct RedisCommand *out);
void free_command(struct RedisCommand *cmd);
#endif