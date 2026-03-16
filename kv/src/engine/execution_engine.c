#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"


// prototypes
ExecuteResult sendOK(int clientfd);
ExecuteResult sendError(int clientfd, char *message);
ExecuteResult sendNotFound(int clientfd);
ExecuteResult sendInt(int clientfd, int integerToSend);
ExecuteResult sendBulkString(int clientfd, const char *data, size_t data_len);
ExecuteResult sendBulkArray(int clientfd, const RedisObject **items, int count);
static ExecuteResult _sendRaw(int clientfd, const char *buff, size_t len);
static ExecuteResult _parseDouble(BulkString *str, double *out);
static ExecuteResult _validate_key_sizes(int clientfd, RedisCommand *command, const struct CommandEntry *entry);

// Command handler prototypes
static ExecuteResult exec_get(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_del(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_zadd(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_zscore(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_zrem(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_zrange(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_ping(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_command(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_flush(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_wait(int clientfd, RedisCommand *command, RedisStore *store);

static struct CommandEntry commandTable[] = {
    // Command    Len  Arity  Flags                                      First Last Step  Handler
    
    // Key/Value Operations
    {"get",       3,   2,    CMD_FLAG_READONLY | CMD_FLAG_FAST,          1,    1,   1,    exec_get},
    {"set",       3,   3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_set},
    {"del",       3,  -2,    CMD_FLAG_WRITE,                             1,   -1,   1,    exec_del},
    
    // Sorted Set (ZSet) Operations
    {"zadd",      4,  -4,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_zadd},
    {"zscore",    6,   3,    CMD_FLAG_READONLY | CMD_FLAG_FAST,          1,    1,   1,    exec_zscore},
    {"zrem",      4,  -3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_zrem},
    {"zrange",    6,  -4,    CMD_FLAG_READONLY,                          1,    1,   1,    exec_zrange},
    
    // Administrative / Utility Commands
    {"ping",      4,   1,    CMD_FLAG_FAST,                              0,    0,   0,    exec_ping},
    {"command",   7,   -1,   CMD_FLAG_READONLY,                          0,    0,   0,    exec_command}, 
    {"flushdb",   7,   1,    CMD_FLAG_ADMIN | CMD_FLAG_WRITE,            0,    0,   0,    exec_flush},
    {"wait",      4,   3,    0,                                          0,    0,   0,    exec_wait},
};

static struct FlagMap flagMap[] = {
    {CMD_FLAG_READONLY, "readonly", 8},
    {CMD_FLAG_WRITE,    "write",    5},
    {CMD_FLAG_ADMIN,    "admin",    5},
    {CMD_FLAG_FAST,     "fast",     4}
};

#define FLAG_MAP_SIZE (sizeof(flagMap) / sizeof(flagMap[0]))
#define COMMAND_TABLE_SIZE (sizeof(commandTable) / sizeof(commandTable[0]))

ExecuteResult dispatch_command(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *cmd_str = &command->args[0];
    
    size_t num_commands = sizeof(commandTable) / sizeof(commandTable[0]);
    for(size_t i=0; i < num_commands; i++){
        struct CommandEntry *entry = &commandTable[i];

        if(entry->name_len == cmd_str->len && strncasecmp(cmd_str->data, entry->name, entry->name_len) == 0) {
            if ((entry->arity > 0 && command->arg_count != entry->arity) ||
                (entry->arity < 0 && command->arg_count < (-entry->arity))) {
                // fprintf(stderr, "[ERROR] %s: wrong number of arguments (expected %d, got %d)\n",
                //         entry->name, entry->arity, command->arg_count);
                sendError(clientfd, "Wrong number of arguments");
                return EE_ERR_ARITY;
            }

            ExecuteResult key_res = _validate_key_sizes(clientfd, command, entry);
            if (key_res != EE_OK) {
                return key_res;
            }

            ExecuteResult res = entry->handler(clientfd, command, store);    

            if (res != EE_OK) {
                return res;
            }
            return (entry->flags & CMD_FLAG_WRITE) ? EE_WRITE_OK : EE_OK;
        }
    }
    char msg[256]; 
    snprintf(msg, sizeof(msg), "Unknown command: '%.*s'", (int)cmd_str->len, cmd_str->data);
    sendError(clientfd, msg);
    return EE_COMMAND_NOT_FOUND;
}

static ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *key = &command->args[1];
    BulkString *value = &command->args[2];

    enum RS_RESULT set_status = rs_set(store, key, value);

    switch (set_status){
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
};

static ExecuteResult exec_get(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *key = &command->args[1];

    RedisObject *obj;
    enum RS_RESULT res = rs_get(store, key, &obj);
    if(res == RS_NOT_FOUND){
        sendNotFound(clientfd);
        return EE_OK;
    } else if(res == RS_WRONG_TYPE){
        sendError(clientfd, "Incorrect type. GET only works on key/value pairs");
        return EE_OK;
    } else if(res == RS_ERR || res != RS_OK){
        sendError(clientfd, "Error fetching key");
        return EE_OK;
    }
    sendBulkString(clientfd, (const char*) obj->data, obj->data_len);
    return EE_OK;
}

static ExecuteResult exec_del(int clientfd, RedisCommand *command, RedisStore *store){
    int del_count = 0;
    for(int i=1; i < command->arg_count; i++){
        BulkString *key = &command->args[i];
        if(rs_delete(store, key) == RS_OK){
            del_count += 1;
        } 
    }
    return sendInt(clientfd, del_count);
}

static ExecuteResult exec_zadd(int clientfd, RedisCommand *command, RedisStore *store){
    // arity > 4 guaranteed by dispatcher
    // should prolly add max arity check
    if(command->arg_count % 2 != 0){   
        sendError(clientfd, "Error: Odd arity for ZADD");
        return EE_OK;
    }
    BulkString *key = &command->args[1];
    int added_count = 0;
    for(int i=2; i<command->arg_count; i+=2){
        BulkString *score_str = &command->args[i];
        BulkString *member = &command->args[i+1];
        
        double score;
        if(_parseDouble(score_str, &score) != EE_OK){;
            sendError(clientfd, "Error parsing double");
            return EE_OK;
        }

        enum RS_RESULT status = rs_zadd(store, key, member, score);
        if(status == RS_WRONG_TYPE){
            sendError(clientfd, "Key belongs to a key/value pair!");
            return EE_OK;
        } else if(status == RS_ERR){
            sendError(clientfd, "Error adding a member/score pair");
            return EE_OK;
        } else if(status == RS_ADDED){
            added_count += 1;
        }
    }
    sendInt(clientfd, added_count);
    return EE_OK;
}

static ExecuteResult exec_zscore(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *key = &command->args[1];
    BulkString *member = &command->args[2];

    double score;
    enum RS_RESULT res = rs_zscore(store, key, member, &score);
    if(res == RS_NOT_FOUND){
        sendNotFound(clientfd);
        return EE_OK;
    } else if(res == RS_WRONG_TYPE){
        sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if(res == RS_ERR || res != RS_OK){
        sendError(clientfd, "Internal Error");
        return EE_OK;
    }
    char buff[64];
    int len = snprintf(buff, sizeof(buff), "%g", score);
    sendBulkString(clientfd, buff, len);
    return EE_OK;
}

static ExecuteResult exec_zrem(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *key_str = &command->args[1];

    Zset *zset = NULL;
    enum RS_RESULT zset_search = rs_get_zset(store, key_str, &zset);
    if (zset_search == RS_NOT_FOUND) {
        sendInt(clientfd, 0);
        return EE_OK;
    } else if (zset_search == RS_WRONG_TYPE) {
        sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if (zset_search != RS_OK) {
        sendError(clientfd, "Internal server error");
        return EE_OK;
    }
    
    int removed_count = 0;
    for(int i = 2; i < command->arg_count; i++) {
        BulkString *member_str = &command->args[i];
        if (rs_zset_remove_member(zset, member_str) == RS_OK) {
            removed_count++;
        }
    }

    sendInt(clientfd, removed_count);
    return EE_OK;
}

static ExecuteResult exec_zrange(int clientfd, RedisCommand *command, RedisStore *store){
    (void) clientfd;
    (void) command;
    (void) store;
    return 0;
};

static ExecuteResult exec_ping(int clientfd, RedisCommand *command, RedisStore *store){
    (void)store;
    if(command->arg_count > 1){
        BulkString *data_str = &command->args[1];
        return sendBulkString(clientfd, data_str->data, data_str->len);
    }
    const char *pong = "+PONG\r\n";
    return _sendRaw(clientfd, pong, 7);
}

static ExecuteResult exec_command(int clientfd, RedisCommand *command, RedisStore *store){
    (void)store;
    if (command->arg_count > 1) {   // tmp stub to keep cli happy
        BulkString *subcommand = &command->args[1];
        if (strncasecmp(subcommand->data, "docs", subcommand->len) == 0) {
            return sendArrayHeader(clientfd, 0);
        }
        return sendArrayHeader(clientfd, 0); 
    }

    int num_commands = COMMAND_TABLE_SIZE;
    ExecuteResult res = sendArrayHeader(clientfd, num_commands);
    if(res != EE_OK) return res;

    for(int i=0; i < num_commands; i++){
        struct CommandEntry *cmd = &commandTable[i];
        ExecuteResult res = sendArrayHeader(clientfd, 10);
        if(res != EE_OK) return res;
        
        // cmd name
        res = sendBulkString(clientfd, cmd->name, cmd->name_len);
        if(res != EE_OK) return res;
        
        // Arity
        res = sendInt(clientfd, cmd->arity);
        if(res != EE_OK) return res;

        // Flags
        int set_flags = __builtin_popcount(cmd->flags);
        res = sendArrayHeader(clientfd, set_flags);
        if(res != EE_OK) return res;
        for(u_long j=0; j<FLAG_MAP_SIZE; j++){
            if(cmd->flags & flagMap[j].mask){
                res = sendBulkString(clientfd, flagMap[j].name, flagMap[j].len);
                if(res != EE_OK) return res;
            }
        }

        // first key
        res = sendInt(clientfd, cmd->first_key);
        if(res != EE_OK) return res;

        // last key
        res = sendInt(clientfd, cmd->last_key);
        if(res != EE_OK) return res;

        // step
        res = sendInt(clientfd, cmd->step);
        if(res != EE_OK) return res;

        // ACL Category stub
        sendArrayHeader(clientfd, 0);
        
        // Command Tips stub
        sendArrayHeader(clientfd, 0);
        
        // Key specifications stub
        sendArrayHeader(clientfd, 0);

        // Subcommands  stub
        sendArrayHeader(clientfd, 0);
    }
    return EE_OK;
}

static ExecuteResult exec_flush(int clientfd, RedisCommand *command, RedisStore *store){
    (void) clientfd;
    (void) command;
    (void) store;
    return 0;
}

static ExecuteResult exec_wait(int clientfd, RedisCommand *command, RedisStore *store){
    (void) clientfd;
    (void) command;
    (void) store;
    return 0;
}


// -------------- HELPERS -------------- //
ExecuteResult sendOK(int clientfd){
    if(clientfd == -1){ // dummy client for AOF replay
        return EE_OK;
    }
    char *buff = "+OK\r\n";
    return _sendRaw(clientfd, buff, 5);
}

ExecuteResult sendError(int clientfd, char *message){
    if(clientfd == -1){ 
        return EE_OK;
    }
    char response[256];
    if(snprintf(response, sizeof(response), "-ERR %s\r\n", message) < 0){
        return EE_ERR; 
    }
    return _sendRaw(clientfd, response, strlen(response));
}

ExecuteResult sendNotFound(int clientfd){
    if(clientfd == -1){ 
        return EE_OK;
    }
    char *buff = "$-1\r\n";
    return _sendRaw(clientfd, buff, 5);
}

ExecuteResult sendInt(int clientfd, int integerToSend){
    if(clientfd == -1){ 
        return EE_OK;
    }
    char buf[23];
    int len = snprintf(buf, sizeof(buf), ":%d\r\n", integerToSend);
    return _sendRaw(clientfd, buf, len);
}

ExecuteResult sendArrayHeader(int clientfd, int count){
    if(clientfd == -1){ 
        return EE_OK;
    }
    char h_buff[32];    
    int h_len = snprintf(h_buff, sizeof(h_buff),"*%d\r\n", count);
    if (h_len < 0 || (u_long) h_len >= sizeof(h_buff)) return EE_ERR;

    ExecuteResult res = _sendRaw(clientfd, h_buff, h_len);
    if(res != EE_OK) return res;    // propagate up
    return EE_OK;
}

ExecuteResult sendBulkArray(int clientfd, const RedisObject **items, int count){
    if(clientfd == -1){ 
        return EE_OK;
    }
    ExecuteResult res = sendArrayHeader(clientfd, count);
    if(res != EE_OK) return res;    // propagate up
    for(int i = 0; i < count; i++){
        ExecuteResult str_sent = sendBulkString(clientfd, items[i]->data, items[i]->data_len);
        if(str_sent != EE_OK) return str_sent;
    }
    return EE_OK;
}

ExecuteResult sendBulkString(int clientfd, const char *data, size_t data_len){
    if(clientfd == -1){ 
        return EE_OK;
    }
    char b_buff[32];

    int b_len = snprintf(b_buff, sizeof(b_buff), "$%zu\r\n", data_len);
    if (b_len < 0 || (u_long) b_len >= sizeof(b_buff)) return EE_ERR;

    ExecuteResult size_sent = _sendRaw(clientfd, b_buff, b_len);
    if(size_sent != EE_OK) return size_sent;   
    
    ExecuteResult data_sent = _sendRaw(clientfd, data, data_len);
    if(data_sent != EE_OK) return data_sent;

    const char *rn = "\r\n";
    ExecuteResult rn_sent = _sendRaw(clientfd, rn, 2);
    if(rn_sent != EE_OK) return rn_sent;
    return EE_OK;
}

static ExecuteResult _sendRaw(int clientfd, const char *buff, size_t len){
    if(clientfd == -1){ 
        return EE_OK;
    }
    size_t total_sent = 0;
    while(total_sent < len){
        ssize_t n = send(clientfd, buff + total_sent, len - total_sent, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EINTR) continue; 
            if (errno == EAGAIN) break;
            // fprintf(stderr, "[ERROR] send() failed: %s (errno=%d)\n", strerror(errno), errno);
            return EE_ERR;
        } else if(n == 0){
            return EE_SOCK_CLOSED;
        }

        total_sent += (size_t) n;
    }
    return EE_OK;
}

ExecuteResult _parseDouble(BulkString *str, double *out) {
    char *endptr;
    char temp[64];
    
    if (str->len >= sizeof(temp)) {
        return EE_ERR;
    }
    
    memcpy(temp, str->data, str->len);
    temp[str->len] = '\0';
    
    errno = 0;
    *out = strtod(temp, &endptr);
    
    if (errno != 0 || endptr == temp || *endptr != '\0') {
        return EE_ERR;
    }
    
    return EE_OK;
}

static ExecuteResult _validate_key_sizes(int clientfd, RedisCommand *command, const struct CommandEntry *entry) {
    if (entry->first_key <= 0 || entry->step == 0) {
        return EE_OK;
    }

    int first_key = entry->first_key;
    int last_key = entry->last_key;
    int step = (entry->step > 0) ? entry->step : 1;

    if (last_key < 0) {
        last_key = command->arg_count + last_key;
    }

    if (last_key >= command->arg_count) {
        last_key = command->arg_count - 1;
    }

    if (first_key >= command->arg_count || last_key < first_key) {
        return EE_OK;
    }

    for (int key_idx = first_key; key_idx <= last_key; key_idx += step) {
        BulkString *key = &command->args[key_idx];
        if (key->len > MAX_KEY_SIZE) {
            sendError(clientfd, "Key size too big");
            return EE_KEY_TOO_LONG;
        }
    }

    return EE_OK;
}