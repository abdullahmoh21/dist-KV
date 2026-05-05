#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"

ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key   = &command->args[1];
    BulkString *value = &command->args[2];

    enum RS_RESULT set_status = rs_set(store, key, value);

    switch (set_status) {
        case RS_OK:
            sendOK(clientfd);
            return EE_OK;
        case RS_OOM:
            sendError(clientfd, "Server ran out of memory");
            return EE_OOM;
        default:
            sendError(clientfd, "Error adding value");
            return EE_ERR;
    }
}

ExecuteResult exec_get(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *key = &command->args[1];

    RedisObject *obj;
    enum RS_RESULT res = rs_get(store, key, &obj);
    if (res == RS_NOT_FOUND) {
        sendNotFound(clientfd);
        return EE_OK;
    } else if (res == RS_WRONG_TYPE) {
        sendError(clientfd, "Incorrect type. GET only works on key/value pairs");
        return EE_OK;
    } else if (res == RS_ERR || res != RS_OK) {
        sendError(clientfd, "Error fetching key");
        return EE_OK;
    }
    sendBulkString(clientfd, (const char *)obj->data, obj->data_len);
    return EE_OK;
}

ExecuteResult exec_del(int clientfd, RedisCommand *command, RedisStore *store) {
    int del_count = 0;
    for (int i = 1; i < command->arg_count; i++) {
        BulkString *key = &command->args[i];
        if (rs_delete(store, key) == RS_OK) {
            del_count++;
        }
    }
    return sendInt(clientfd, del_count);
}
