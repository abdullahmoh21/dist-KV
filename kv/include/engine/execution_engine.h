#ifndef EXECUTE_ENGINE_H
#define EXECUTE_ENGINE_H

#include <stddef.h>    
#include <store/object.h>
#include <sys/types.h>  

// Forward declarations
typedef struct RedisCommand RedisCommand;
typedef struct RedisStore RedisStore;

typedef enum { 
    EE_OK,
    EE_WRITE_OK,
    EE_OOM,
    EE_COMMAND_NOT_FOUND,
    EE_ERR,
    EE_SOCK_CLOSED,
    EE_KEY_TOO_LONG,
    EE_ERR_ARITY,
} ExecuteResult;

#define MAX_KEY_SIZE (25 * 1024 * 1024)

// Command flags
#define CMD_FLAG_READONLY  (1 << 0)
#define CMD_FLAG_WRITE     (1 << 1)
#define CMD_FLAG_ADMIN     (1 << 2)
#define CMD_FLAG_FAST      (1 << 3)

struct FlagMap {
    uint32_t mask;
    const char *name;
    size_t len;
};


typedef ExecuteResult (*CommandFunc)(int clientfd, struct RedisCommand *cmd, RedisStore *store);
typedef ExecuteResult (*ReplyWriteFn)(int clientfd, const char *data, size_t len, void *ctx);
struct CommandEntry {
    const char *name;       
    size_t name_len;        
    int arity;              
    uint32_t flags;              
    int first_key;          
    int last_key;           
    int step;               
    CommandFunc handler;    
};

ExecuteResult dispatch_command(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult sendOK(int clientfd);
ExecuteResult sendError(int clientfd, char *message);
ExecuteResult sendNotFound(int clientfd);
ExecuteResult sendInt(int clientfd, int integerToSend);
ExecuteResult sendArrayHeader(int clientfd, int count);
ExecuteResult sendBulkString(int clientfd, const char *data, size_t data_len);
ExecuteResult sendBulkArray(int clientfd, const RedisObject **items, int count);
void ee_set_reply_writer(ReplyWriteFn writer, void *ctx);
#endif
