#ifndef EXECUTE_ENGINE_H
#define EXECUTE_ENGINE_H

#include <stddef.h>    
#include <sys/types.h>  

// Forward declarations
typedef struct RedisCommand RedisCommand;
typedef struct RedisStore RedisStore;

typedef enum { 
    EE_OK,
    EE_OOM,
    EE_COMMAND_NOT_FOUND,
    EE_ERR,
    EE_SOCK_CLOSED,
    EE_KEY_TOO_LONG,
    EE_ERR_ARITY,
} ExecuteResult;

#define MAX_KEY_SIZE (25 * 1024 * 1024)

typedef ExecuteResult (*CommandFunc)(int clientfd, struct RedisCommand *cmd, RedisStore *store);

struct CommandEntry {
    const char *name;    
    size_t name_len;  
    int arity;             
    CommandFunc handler;   
};

ExecuteResult execute_command(int clientfd, RedisCommand *command, RedisStore *store);
void free_command(struct RedisCommand *command);

#endif
