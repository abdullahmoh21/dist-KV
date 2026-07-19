#include <stdio.h>
#include <string.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"

// Handler forward declarations
ExecuteResult exec_get(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_set(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_del(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_incr(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_decr(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_incrby(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_decrby(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_expire(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_pexpire(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_pexpireat(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_ttl(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_pttl(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_persist(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_zadd(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_zscore(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_zrem(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_zrange(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_zpopmin(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_bzpopmin(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_ping(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_flush(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_wait(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_replconf(int clientfd, RedisCommand *command, RedisStore *store);
ExecuteResult exec_psync(int clientfd, RedisCommand *command, RedisStore *store);

static ExecuteResult exec_command(int clientfd, RedisCommand *command, RedisStore *store);
static ExecuteResult _validate_key_sizes(int clientfd, RedisCommand *command, const struct CommandEntry *entry);

static struct CommandEntry commandTable[] = {
    // Command    Len  Arity  Flags                                      First Last Step  Handler

    // Key/Value Operations
    {"get",       3,   2,    CMD_FLAG_READONLY | CMD_FLAG_FAST,          1,    1,   1,    exec_get},
    {"set",       3,  -3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_set},
    {"del",       3,  -2,    CMD_FLAG_WRITE,                             1,   -1,   1,    exec_del},
    {"incr",      4,   2,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_incr},
    {"decr",      4,   2,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_decr},
    {"incrby",    6,   3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_incrby},
    {"decrby",    6,   3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_decrby},

    // Expiry / TTL Operations
    {"expire",    6,   3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_expire},
    {"pexpire",   7,   3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_pexpire},
    {"pexpireat", 9,   3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_pexpireat},
    {"ttl",       3,   2,    CMD_FLAG_READONLY | CMD_FLAG_FAST,          1,    1,   1,    exec_ttl},
    {"pttl",      4,   2,    CMD_FLAG_READONLY | CMD_FLAG_FAST,          1,    1,   1,    exec_pttl},
    {"persist",   7,   2,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_persist},

    // Sorted Set (ZSet) Operations
    {"zadd",      4,  -4,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_zadd},
    {"zscore",    6,   3,    CMD_FLAG_READONLY | CMD_FLAG_FAST,          1,    1,   1,    exec_zscore},
    {"zrem",      4,  -3,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_zrem},
    {"zrange",    6,  -4,    CMD_FLAG_READONLY,                          1,    1,   1,    exec_zrange},
    {"zpopmin",   7,  -2,    CMD_FLAG_WRITE | CMD_FLAG_FAST,             1,    1,   1,    exec_zpopmin},
    // last_key -2: the trailing arg is a timeout, not a key.
    {"bzpopmin",  8,  -3,    CMD_FLAG_WRITE,                             1,   -2,   1,    exec_bzpopmin},

    // Administrative / Utility Commands
    {"ping",      4,   1,    CMD_FLAG_FAST,                              0,    0,   0,    exec_ping},
    {"command",   7,   -1,   CMD_FLAG_READONLY,                          0,    0,   0,    exec_command},
    {"flushdb",   7,   1,    CMD_FLAG_ADMIN | CMD_FLAG_WRITE,            0,    0,   0,    exec_flush},
    {"wait",      4,   3,    0,                                          0,    0,   0,    exec_wait},
    {"replconf",  8,  -3,    CMD_FLAG_ADMIN,                             0,    0,   0,    exec_replconf},
    {"psync",     5,   3,    CMD_FLAG_ADMIN,                             0,    0,   0,    exec_psync},
};

static struct FlagMap flagMap[] = {
    {CMD_FLAG_READONLY, "readonly", 8},
    {CMD_FLAG_WRITE,    "write",    5},
    {CMD_FLAG_ADMIN,    "admin",    5},
    {CMD_FLAG_FAST,     "fast",     4}
};

#define FLAG_MAP_SIZE    (sizeof(flagMap)    / sizeof(flagMap[0]))
#define COMMAND_TABLE_SIZE (sizeof(commandTable) / sizeof(commandTable[0]))

ExecuteResult dispatch_command(int clientfd, RedisCommand *command, RedisStore *store) {
    BulkString *cmd_str = &command->args[0];

    size_t num_commands = COMMAND_TABLE_SIZE;
    for (size_t i = 0; i < num_commands; i++) {
        struct CommandEntry *entry = &commandTable[i];

        if (entry->name_len == cmd_str->len && strncasecmp(cmd_str->data, entry->name, entry->name_len) == 0) {
            if ((entry->arity > 0 && command->arg_count != entry->arity) ||
                (entry->arity < 0 && command->arg_count < (-entry->arity))) {
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

static ExecuteResult exec_command(int clientfd, RedisCommand *command, RedisStore *store) {
    (void)store;
    if (command->arg_count > 1) {
        BulkString *subcommand = &command->args[1];
        if (strncasecmp(subcommand->data, "docs", subcommand->len) == 0) {
            return sendArrayHeader(clientfd, 0);
        }
        return sendArrayHeader(clientfd, 0);
    }

    int num_commands = COMMAND_TABLE_SIZE;
    ExecuteResult res = sendArrayHeader(clientfd, num_commands);
    if (res != EE_OK) return res;

    for (int i = 0; i < num_commands; i++) {
        struct CommandEntry *cmd = &commandTable[i];
        res = sendArrayHeader(clientfd, 10);
        if (res != EE_OK) return res;

        res = sendBulkString(clientfd, cmd->name, cmd->name_len);
        if (res != EE_OK) return res;

        res = sendInt(clientfd, cmd->arity);
        if (res != EE_OK) return res;

        int set_flags = __builtin_popcount(cmd->flags);
        res = sendArrayHeader(clientfd, set_flags);
        if (res != EE_OK) return res;
        for (u_long j = 0; j < FLAG_MAP_SIZE; j++) {
            if (cmd->flags & flagMap[j].mask) {
                res = sendBulkString(clientfd, flagMap[j].name, flagMap[j].len);
                if (res != EE_OK) return res;
            }
        }

        res = sendInt(clientfd, cmd->first_key);
        if (res != EE_OK) return res;
        res = sendInt(clientfd, cmd->last_key);
        if (res != EE_OK) return res;
        res = sendInt(clientfd, cmd->step);
        if (res != EE_OK) return res;

        sendArrayHeader(clientfd, 0); // ACL Category stub
        sendArrayHeader(clientfd, 0); // Command Tips stub
        sendArrayHeader(clientfd, 0); // Key specifications stub
        sendArrayHeader(clientfd, 0); // Subcommands stub
    }
    return EE_OK;
}

static ExecuteResult _validate_key_sizes(int clientfd, RedisCommand *command, const struct CommandEntry *entry) {
    if (entry->first_key <= 0 || entry->step == 0) {
        return EE_OK;
    }

    int first_key = entry->first_key;
    int last_key  = entry->last_key;
    int step      = (entry->step > 0) ? entry->step : 1;

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
