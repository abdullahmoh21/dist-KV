#ifndef REDIS_PROTOCOL_H
#define REDIS_PROTOCOL_H

#include <stddef.h>    
#include <sys/types.h>  

// Forward Declaration
struct InputBuffer;

/* Enums and Structs */
enum ParseResult { 
    PARSE_OK, 
    PARSE_NEED_MORE, 
    PARSE_ERR 
};

struct BulkString {
    void *data;
    size_t len;
};

struct RedisCommand {
    struct BulkString *args;
    size_t arg_count;
};

/* Function Prototypes */
int parse_array_command(char *buff, size_t buff_len, struct RedisCommand *out);

#endif