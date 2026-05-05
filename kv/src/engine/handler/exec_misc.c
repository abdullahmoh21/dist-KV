#include <string.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "aof/aof.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"

ExecuteResult exec_ping(int clientfd, RedisCommand *command, RedisStore *store) {
    (void)store;
    if (command->arg_count > 1) {
        BulkString *data_str = &command->args[1];
        return sendBulkString(clientfd, data_str->data, data_str->len);
    }
    return sendSimpleString(clientfd, "PONG", 4);
}

ExecuteResult exec_flush(int clientfd, RedisCommand *command, RedisStore *store) {
    (void)command;
    if (rs_flush(store) != RS_OK) {
        return sendError(clientfd, "ERR flush failed");
    }
    if (store->aof) {
        aof_truncate(store->aof);
    }
    return sendSimpleString(clientfd, "OK", 2);
}

ExecuteResult exec_wait(int clientfd, RedisCommand *command, RedisStore *store) {
    (void)clientfd;
    (void)command;
    (void)store;
    return 0;
}

