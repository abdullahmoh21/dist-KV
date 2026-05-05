#include <string.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"



ExecuteResult exec_replconf(int clientfd, RedisCommand *cmd, RedisStore *store) {
    (void)cmd;
    (void)store;
    return sendOK(clientfd);    // client can fuck off for now
}


ExecuteResult exec_psync(int clientfd, RedisCommand *cmd, RedisStore *store) {

}
