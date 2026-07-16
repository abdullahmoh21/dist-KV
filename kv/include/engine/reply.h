#ifndef REPLY_H
#define REPLY_H

#include <stddef.h>
#include "engine/execution_engine.h"
#include "store/object.h"

ExecuteResult sendOK(int clientfd);
ExecuteResult sendSimpleString(int clientfd, const char *str, size_t len);
ExecuteResult sendError(int clientfd, char *message);
ExecuteResult sendNotFound(int clientfd);
ExecuteResult sendInt(int clientfd, int integerToSend);
ExecuteResult sendInt64(int clientfd, long long integerToSend);
ExecuteResult sendArrayHeader(int clientfd, int count);
ExecuteResult sendBulkString(int clientfd, const char *data, size_t data_len);
ExecuteResult sendBulkArray(int clientfd, const RedisObject **items, int count);

#endif
