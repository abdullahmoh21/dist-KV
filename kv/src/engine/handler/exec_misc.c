#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include "parser/resp_parser.h"
#include "store/redis_store.h"
#include "aof/aof.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"
#include "replication/replication.h"

// Placeholder replication ID — replaced by a real generated ID in a future iteration.
static const char REPL_ID[] = "0000000000000000000000000000000000000000";

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

ExecuteResult exec_replconf(int clientfd, RedisCommand *command, RedisStore *store) {
    (void)store;

    // REPLCONF ACK <offset> — replica acknowledging bytes received; no reply sent back.
    if (command->arg_count >= 2) {
        BulkString *sub = &command->args[1];
        if (sub->len == 3 && strncasecmp(sub->data, "ack", 3) == 0) {
            if (command->arg_count >= 3) {
                uint64_t off = (uint64_t)strtoull(command->args[2].data, NULL, 10);
                repl_update_ack(clientfd, off);
            }
            return EE_OK;
        }
    }
    int suppress = repl_handle_replconf(clientfd, command);
    if (suppress) return EE_OK;
    return sendSimpleString(clientfd, "OK", 2);
}

ExecuteResult exec_psync(int clientfd, RedisCommand *command, RedisStore *store) {
    (void)command;
    (void)store;
    // Send +FULLRESYNC <repl_id> <current_offset>\r\n.
    // server.c detects EE_PSYNC_FULL_SYNC and forks the snapshot.
    char msg[80];
    int  len = snprintf(msg, sizeof(msg), "FULLRESYNC %s %" PRIu64, REPL_ID, repl_get_offset());
    ExecuteResult r = sendSimpleString(clientfd, msg, (size_t)len);
    if (r != EE_OK) return r;
    return EE_PSYNC_FULL_SYNC;
}

ExecuteResult exec_wait(int clientfd, RedisCommand *command, RedisStore *store) {
    (void)store;

    // WAIT numreplicas timeout_ms
    long numreplicas = strtol(command->args[1].data, NULL, 10);
    long timeout_ms  = strtol(command->args[2].data, NULL, 10);
    if (numreplicas < 0) {
        return sendError(clientfd, "ERR numreplicas must be >= 0");
    }

    // Snapshot the primary offset: replicas must have acknowledged up to this
    // point for their writes to be considered durable.
    uint64_t target = repl_get_offset();
    int synced = repl_count_synced(target);

    // Satisfy immediately if enough replicas are already synced, if the caller
    // requested zero replicas, or if the timeout is 0 (poll-only semantics).
    if (synced >= (int)numreplicas || numreplicas == 0 || timeout_ms == 0) {
        return sendInt(clientfd, synced);
    }

    // Not yet satisfied — signal the server to park a deferred reply.
    // The server reads numreplicas and timeout_ms from the command args directly.
    return EE_WAIT_PENDING;
}

