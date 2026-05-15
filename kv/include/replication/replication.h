#ifndef REPLICATION_H
#define REPLICATION_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <store/buffer.h>

typedef struct RedisCommand RedisCommand;
typedef struct RedisStore   RedisStore;
typedef struct event_loop   event_loop_t;

/* ─── Shared replication backlog (ring buffer) ────────────────────────── */

#define REPL_BACKLOG_DEFAULT_CAP (1ULL * 1024 * 1024)  // 1 MB

typedef struct ReplBacklog {
    char    *data;
    size_t   cap;
    size_t   read_head;    // index of oldest byte in the ring
    size_t   write_head;   // index where the next byte will be written
    size_t   used;         // bytes currently stored
    uint64_t base_offset;  // absolute repl offset of data[read_head]
} ReplBacklog;

// Must be called once before any replicas connect.
void repl_backlog_init(size_t cap);

/* ─── Primary-side: replica tracking ─────────────────────────────────── */

typedef enum {
    REPLICA_HANDSHAKE,          // connected, awaiting REPLCONF / PSYNC
    REPLICA_PSYNC_PENDING,      // PSYNC received while another fork was running; deferred
    REPLICA_FULL_SYNC,          // snapshot fork running; acked cursor frozen at PSYNC offset
    REPLICA_SENDING_SNAPSHOT,   // fork done; streaming snapshot file to replica socket
    REPLICA_STREAMING,          // live write propagation via shared backlog
} ReplicaState;

typedef struct Replica {
    int fd;
    struct Buffer *send_buf;        // points at ClientState.out_buffer — used for snapshot only
    uint64_t offset;                // bytes replica has acknowledged (REPLCONF ACK → used by WAIT)
    uint64_t acked;                 // bytes sent to replica socket (ring read cursor)
    ReplicaState state;
    int dead;                       // set to 1 on overflow/eviction; cleaned up by server
    pid_t sync_child_pid;           // snapshot fork PID (-1 when idle)
    char sync_file[64];             // e.g. "fullsync_7.aof"
    int sync_send_fd;               // snapshot file fd being streamed (-1 when idle)
    int64_t snapshot_bytes_remaining;
    uint16_t listening_port;
} Replica;

// Register server-level pointers so replication helpers can read/write them.
void repl_set_context(Replica ***replicas, int *replica_count, uint64_t *repl_offset);

uint64_t repl_get_offset(void);
int      repl_count_synced(uint64_t target_offset);

// Lifecycle — start_acked is *g_repl_offset at the moment PSYNC was received.
int      repl_add_replica(int fd, struct Buffer *send_buf, uint64_t start_acked);
void     repl_remove_replica(int fd);
Replica *repl_find_replica(int fd);

// Drain the backlog slice for replica r directly to its socket.
// Returns bytes sent (≥0) or -1 if the connection should be closed.
int  repl_backlog_drain_replica(Replica *r);
// True when the backlog has unsent data for r.
int  repl_backlog_has_data(const Replica *r);

// Called on every EE_WRITE_OK command to fan out to all replicas.
void repl_propagate(RedisCommand *cmd);

// Called each event loop tick to advance snapshot streaming.
// Returns the count of snapshot forks that completed this tick.
int  repl_check_sync_children(event_loop_t *loop);
void repl_advance_snapshot_send(event_loop_t *loop);

// Handlers called by exec_replconf / exec_psync.
// Returns 0 on success, 1 if the caller should suppress the +OK reply (ACK case).
int repl_handle_replconf(int clientfd, RedisCommand *cmd);
void repl_update_ack(int fd, uint64_t ack_offset);

/* ─── Replica-side: state machine when running as --replicaof ─────── */

typedef enum {
    REPL_CLIENT_CONNECTING,       // waiting for non-blocking connect to complete
    REPL_CLIENT_WAIT_PONG,        // sent PING, waiting +PONG
    REPL_CLIENT_WAIT_OK_1,        // sent REPLCONF listening-port, waiting +OK
    REPL_CLIENT_WAIT_OK_2,        // sent REPLCONF capa psync2, waiting +OK
    REPL_CLIENT_WAIT_FULLRESYNC,  // sent PSYNC ? -1, waiting +FULLRESYNC
    REPL_CLIENT_RECV_BULK_HDR,    // received +FULLRESYNC, waiting $<size>
    REPL_CLIENT_RECV_BULK,        // reading snapshot bulk data
    REPL_CLIENT_STREAMING,        // live streaming from primary
} ReplClientState;

typedef struct ReplClientContext {
    int fd;
    ReplClientState state;
    uint16_t our_port;
    char repl_id[41];
    uint64_t repl_offset;       // bytes received + applied from primary
    int64_t  bulk_remaining;    // bytes left in snapshot bulk transfer
    struct Buffer *recv_buf;    // data arriving from primary
    struct Buffer *send_buf;    // PING / REPLCONF / ACK outbound to primary
    uint64_t last_ack_ms;
} ReplClientContext;

ReplClientContext *replica_client_create(int fd, uint16_t our_port);
void replica_client_destroy(ReplClientContext *ctx);
// Returns 0 on success, -1 if the primary connection was lost (caller must clean up).
int replica_client_handle_readable(ReplClientContext *ctx, RedisStore *store, event_loop_t *loop);
int replica_client_handle_writable(ReplClientContext *ctx, event_loop_t *loop);
void replica_client_send_ack(ReplClientContext *ctx);

#endif
