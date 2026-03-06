#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"


// prototypes
static ExecuteResult _sendOK(int clientfd);
static ExecuteResult _sendError(int clientfd, char *message);
static ExecuteResult _sendNotFound(int clientfd);
static ExecuteResult _sendInt(int clientfd, int integerToSend);
static ExecuteResult _sendBulkString(int clientfd, const char *data, size_t data_len);
static ExecuteResult _sendRaw(int clientfd, const char *buff, size_t len);
static ExecuteResult _parseDouble(BulkString *str, double *out);
static ExecuteResult _sendBulkArray(int clientfd, const RedisObject **items, int count);

// Command handler prototypes
static ExecuteResult exec_get(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_del(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_zadd(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_zrem(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_zrange(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_ping(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_flush(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_wait(int clientfd, RedisCommand *command, RedisStore *store);

static struct CommandEntry commandTable[] = {
    {"GET", 3,     2,  exec_get},    
    {"SET", 3,     3,  exec_set},    
    {"DEL", 3,    -2,  exec_del},    

    /* Sorted Set (ZSet) Operations */
    {"ZADD",   4,   -4,  exec_zadd},   
    {"ZREM",   4,   -3,  exec_zrem},   
    {"ZRANGE", 6,   -4,  exec_zrange}, 
    
    /* Administrative / Pipeline */
    {"PING",    4,    1,  exec_ping},   
    {"PING",    4,    2,  exec_ping},   
    {"FLUSHDB", 7,    1,  exec_flush},  
    {"WAIT",    4,    1,  exec_wait},  
};

ExecuteResult execute_command(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *cmd_str = &command->args[0];
    
    size_t num_commands = sizeof(commandTable) / sizeof(commandTable[0]);
    for(int i=0; i < num_commands; i++){
        struct CommandEntry *entry = &commandTable[i]; 

        if(entry->name_len == cmd_str->len && strncasecmp(cmd_str->data, entry->name, entry->name_len) == 0) {
            if ((entry->arity > 0 && command->arg_count != entry->arity) ||
                (entry->arity < 0 && command->arg_count < (-entry->arity))) {
                _sendError(clientfd, "ERR wrong number of arguments");
                return EE_ERR_ARITY;
            }

            return entry->handler(clientfd, command, store);    
        }
    }
    return EE_COMMAND_NOT_FOUND;
}

static ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *key = &command->args[1];
    BulkString *value = &command->args[2];
    
    if(key->len > MAX_KEY_SIZE){
        _sendError(clientfd, "Key size too big");
        return EE_OK;
    }

    enum RS_RESULT set_status = rs_set(store, key, value);

    switch (set_status){
        case RS_OK:
            _sendOK(clientfd);
            return EE_OK;
        case RS_OOM:
            _sendError(clientfd, "Server ran out of memory");
            return EE_OOM;
        default:
            _sendError(clientfd, "Error adding value");
            return EE_ERR;
    }   
};

static ExecuteResult exec_get(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *key = &command->args[1];
    if((int) key->len > MAX_KEY_SIZE){
        _sendError(clientfd, "Key size too big");
        return EE_OK;
    }

    RedisObject *obj;
    enum RS_RESULT res = rs_get(store, key, &obj);
    if(res == RS_NOT_FOUND){
        _sendNotFound(clientfd);
        return EE_OK;
    } else if(res == RS_WRONG_TYPE){
        _sendError(clientfd, "Incorrect type. GET only works on key/value pairs");
        return EE_OK;
    } else if(res == RS_ERR || res != RS_OK){
        _sendError(clientfd, "Error fetching key");
        return EE_OK;
    }
    _sendBulkString(clientfd, (const char*) obj->data, obj->data_len);
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
    return _sendInt(clientfd, del_count);
}

static ExecuteResult exec_zadd(int clientfd, RedisCommand *command, RedisStore *store){
     // arity > 4 guaranteed by dispatcher
    if(command->arg_count % 2 != 0){   
        _sendError(clientfd, "Error: Odd arity for ZADD");
        return EE_OK;
    }
    BulkString *key = &command->args[1];
    int added_count = 0;
    for(int i=2; i<command->arg_count; i+=2){
        BulkString *score_str = &command->args[i];
        BulkString *member = &command->args[i+1];
        
        double score;
        if(_parseDouble(score_str, &score) != EE_OK){;
            _sendError(clientfd, "Error parsing double");
            return EE_OK;
        }

        enum RS_RESULT status = rs_zadd(store, key, member, score);
        if(status == RS_WRONG_TYPE){
            _sendError(clientfd, "Key belongs to a key/value pair!");
            return EE_OK;
        } else if(status == RS_ERR){
            _sendError(clientfd, "Error adding a member/score pair");
            return EE_OK;
        } else if(status == RS_ADDED){
            added_count += 1;
        }
    }
    _sendInt(clientfd, added_count);
    return EE_OK;
}

static ExecuteResult exec_zrem(int clientfd, RedisCommand *command, RedisStore *store){
    BulkString *key_str = &command->args[1];

    Zset *zset = NULL;
    enum RS_RESULT zset_search = rs_get_zset(store, key_str, &zset);
    if (zset_search == RS_NOT_FOUND) {
        _sendInt(clientfd, 0);
        return EE_OK;
    } else if (zset_search == RS_WRONG_TYPE) {
        _sendError(clientfd, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return EE_OK;
    } else if (zset_search != RS_OK) {
        _sendError(clientfd, "Internal server error");
        return EE_OK;
    }
    
    int removed_count = 0;
    for(int i = 2; i < command->arg_count; i++) {
        BulkString *member_str = &command->args[i];
        if (rs_zset_remove_member(zset, member_str) == RS_OK) {
            removed_count++;
        }
    }

    _sendInt(clientfd, removed_count);
    return EE_OK;
}

static ExecuteResult exec_zrange(int clientfd, RedisCommand *command, RedisStore *store);

static ExecuteResult exec_ping(int clientfd, RedisCommand *command, RedisStore *store){
    if(command->arg_count > 1){
        BulkString *data_str = &command->args[1];
        return _sendBulkString(clientfd, data_str->data, data_str->len);
    }
    const char *pong = "+PONG\r\n";
    return _sendRaw(clientfd, pong, 7);
}

static ExecuteResult exec_flush(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult exec_wait(int clientfd, RedisCommand *command, RedisStore *store);


// -------------- HELPERS -------------- //

static ExecuteResult _sendOK(int clientfd){
    char *buff = "+OK\r\n";
    return _sendRaw(clientfd, buff, 5);
}

static ExecuteResult _sendError(int clientfd, char *message){
    char response[256];
    if(snprintf(response, sizeof(response), "-ERR %s\r\n", message) < 0){
        return EE_ERR; 
    }
    return _sendRaw(clientfd, response, strlen(response));
}

static ExecuteResult _sendNotFound(int clientfd){
    char *buff = "$-1\r\n";
    return _sendRaw(clientfd, buff, 5);
}

static ExecuteResult _sendInt(int clientfd, int integerToSend){
    char buf[23];
    int len = snprintf(buf, sizeof(buf), ":%d\r\n", integerToSend);
    return _sendRaw(clientfd, buf, len);
}

static ExecuteResult _sendBulkArray(int clientfd, const RedisObject **items, int count){
    char h_buff[32];
    int h_len = snprintf(h_buff, sizeof(h_buff),"*%d\r\n", count);
    if (h_len < 0 || h_len >= sizeof(h_buff)) return EE_ERR;

    ExecuteResult res = _sendRaw(clientfd, h_buff, h_len);
    if(res != EE_OK) return res;    // propagate up
    
    for(int i = 0; i < count; i++){
        ExecuteResult str_sent = _sendBulkString(clientfd, items[i]->data, items[i]->data_len);
        if(str_sent != EE_OK) return str_sent;
    }
    return EE_OK;
}

static ExecuteResult _sendBulkString(int clientfd, const char *data, size_t data_len){
    char b_buff[32];

    int b_len = snprintf(b_buff, sizeof(b_buff), "$%zu\r\n", data_len);
    if (b_len < 0 || b_len >= sizeof(b_buff)) return EE_ERR;

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
    size_t total_sent = 0;
    while(total_sent < len){
        ssize_t n = send(clientfd, buff + total_sent, len - total_sent, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EINTR) continue; 
            if (errno == EAGAIN) break;   
            return EE_ERR;
        } else if(n == 0){
            return EE_SOCK_CLOSED;
        }

        total_sent += (size_t) n;
    }
    return EE_OK;
}

static ExecuteResult _parseDouble(BulkString *str, double *out) {
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