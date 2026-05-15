#include "store/buffer.h"
#include "aof/aof.h"
#include "store/skip_list.h"
#include "store/hashmap.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"
#include "parser/resp_parser.h"
#include "event_loop/event_loop.h"
#include "replication/replication.h"
#include "utils/time.h"
#include <stdio.h>
#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/stat.h>

#define DEFAULT_PORT "6379"
#define BACKLOG 511                        // matches Redis default; avoids SYN drops under burst load
#define EXPANSION_THRESH 10
#define MAX_EVENTS 128
#define INITIAL_OUT_BUFF_CAPACITY 4096
#define MAX_CLIENT_OUTPUT_BUFF_LEN (64 * 1024 * 1024)
// Direct fd-indexed client table: O(1) lookup by file descriptor, no hashing.
// Replaces the HashMap used previously for hot-path client lookups.
// Size must exceed the process's open-file limit; 4096 is safe for typical deployments.
#define MAX_CLIENT_FDS 4096
#define MAX_PENDING_WAITS 256

// Client connection state
typedef struct {
    int fd;
    int event_mask;
    struct Buffer *buffer;
    struct Buffer *out_buffer;
    int output_overflow;
} ClientState;

// A WAIT command that couldn't be satisfied immediately.  The server parks
// it here and resolves it each event loop tick once enough replicas have ACK'd
// or the deadline passes.
typedef struct {
    int      clientfd;
    uint64_t target_offset;   // primary offset at time WAIT was issued
    uint64_t deadline_ms;     // absolute monotonic time when we give up waiting
    int      numreplicas;     // how many replicas the client wants synced
    int      active;          // 1 = slot in use
} PendingWait;

// Server-wide state — single instance, file-static
typedef struct {
    int fd;
    event_loop_t *loop;
    ClientState *fd_clients[MAX_CLIENT_FDS]; // direct fd→client mapping, O(1) hot-path lookup
    RedisStore store;
    AOFManager *aof;
    ClientState *current_client;
    Replica **replicas;
    int replica_count;
    uint64_t repl_offset;       // bytes of write commands propagated to replicas so far
    ReplClientContext *repl_client;  // non-NULL when running in --replicaof mode
    int active_forks;           // running fork children (AOF compact + PSYNC snapshots)
    PendingWait pending_waits[MAX_PENDING_WAITS];
} Server;

static Server server;

// Prototypes
int set_nonblocking(int fd);
void handle_new_connection(int serverfd, event_loop_t *loop, RedisStore *store);
void handle_client_read(int clientfd, event_loop_t *loop, RedisStore *store, AOFManager *aof);
void handle_client_write(int clientfd, event_loop_t *loop);
void close_client(int clientfd, event_loop_t *loop);
void *get_in_addr(struct sockaddr *sa);
static ExecuteResult server_reply_write(int clientfd, const char *data, size_t len, void *ctx);
static int append_client_output(ClientState *client, const char *data, size_t len);
static ExecuteResult flush_client_output(ClientState *client);
static int update_client_event_mask(ClientState *client, event_loop_t *loop);
static void handle_psync_full_sync(int clientfd, event_loop_t *loop);
static void launch_replica_sync(Replica *r, event_loop_t *loop);
static int connect_to_primary(const char *host, const char *port, uint16_t our_port);
static int any_fork_running(void);
static void register_pending_wait(int clientfd, uint64_t target, int numreplicas, long timeout_ms);
static uint64_t next_pending_wait_deadline_ms(void);
static void check_pending_waits(void);
static void cancel_pending_waits(int clientfd);

int main(int argc, char *argv[]){
    const char *server_port   = DEFAULT_PORT;
    const char *replicaof_host = NULL;
    const char *replicaof_port = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i+1 < argc) {
            server_port = argv[++i];
        } else if (strcmp(argv[i], "--replicaof") == 0 && i+2 < argc) {
            replicaof_host = argv[++i];
            replicaof_port = argv[++i];
        }
    }

    struct addrinfo hints, *servinfo, *p;
    int yes = 1;
    int rv;

    // Initialize Redis store
    if(create_store(&server.store) != RS_OK){
        fprintf(stderr, "Failed to create Redis store\n");
        return 1;
    }

    enum AOF_RESULT res = aof_load(&server.store);
    if(res != AOF_OK){
        fprintf(stderr, "Failed to load AOF into memory\n");
        return 1;
    }

    ee_set_reply_writer(server_reply_write, NULL);
    repl_set_context(&server.replicas, &server.replica_count, &server.repl_offset);
    repl_backlog_init(REPL_BACKLOG_DEFAULT_CAP);

    if(aof_create(&server.aof) != AOF_OK){
        fprintf(stderr, "Failed to create AOF buffer\n");
        return 1;
    }
    server.store.aof = server.aof;

    // Setup address info
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if((rv = getaddrinfo(NULL, server_port, &hints, &servinfo)) != 0){
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    // Create and bind server socket
    for(p = servinfo; p != NULL; p = p->ai_next){
        server.fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if(server.fd == -1){
            continue;
        }

        if(setsockopt(server.fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1){
            exit(1);
        }

        if(bind(server.fd, p->ai_addr, p->ai_addrlen) == -1){
            close(server.fd);
            continue;
        }
        break;
    }

    freeaddrinfo(servinfo);

    if(p == NULL){
        fprintf(stderr, "server: failed to bind\n");
        exit(1);
    }

    if(listen(server.fd, BACKLOG) == -1){
        exit(1);
    }

    if(set_nonblocking(server.fd) == -1){
        fprintf(stderr, "Failed to set server socket non-blocking\n");
        close(server.fd);
        return 1;
    }

    server.loop = event_loop_create(MAX_EVENTS);
    if(!server.loop){
        fprintf(stderr, "Failed to create event loop\n");
        close(server.fd);
        return 1;
    }

    if(event_loop_add(server.loop, server.fd, EVENT_READABLE) == -1){
        fprintf(stderr, "Failed to add server socket to event loop\n");
        event_loop_destroy(server.loop);
        close(server.fd);
        return 1;
    }

    printf("server: waiting for connections on port %s\n", server_port);

    if (replicaof_host) {
        uint16_t our_port = (uint16_t)atoi(server_port);
        if (connect_to_primary(replicaof_host, replicaof_port, our_port) != 0) {
            fprintf(stderr, "Failed to connect to primary %s:%s\n",
                    replicaof_host, replicaof_port);
            return 1;
        }
    }

    // Main event loop
    fired_event_t events[MAX_EVENTS];
    uint64_t next_flush_ms = monotonic_ms() + 1000;
    while(1) {
        uint64_t now_ms = monotonic_ms();
        // Wake up by the AOF flush timer or the nearest pending WAIT deadline,
        // whichever comes first, so check_pending_waits fires on time.
        uint64_t next_wakeup_ms = next_flush_ms;
        uint64_t pw_deadline = next_pending_wait_deadline_ms();
        if (pw_deadline < next_wakeup_ms) next_wakeup_ms = pw_deadline;
        int timeout_ms = (next_wakeup_ms > now_ms) ? ((int)(next_wakeup_ms - now_ms)) : 0;
        int n = event_loop_wait(server.loop, timeout_ms, events, MAX_EVENTS);

        if(n < 0){
            if(errno == EINTR) continue;
            break;
        }

        for(int i = 0; i < n; i++){
            int fd = events[i].fd;
            int mask = events[i].mask;

            if(fd == server.fd){
                handle_new_connection(server.fd, server.loop, &server.store);
            } else if(server.repl_client && fd == server.repl_client->fd){
                int rc = 0;
                if(mask & EVENT_WRITABLE){
                    rc = replica_client_handle_writable(server.repl_client, server.loop);
                }
                if(rc == 0 && (mask & EVENT_READABLE)){
                    rc = replica_client_handle_readable(server.repl_client, &server.store, server.loop);
                }
                if(rc < 0){
                    // Primary disconnected — tear down the replica client context
                    event_loop_del(server.loop, server.repl_client->fd,
                                   EVENT_READABLE | EVENT_WRITABLE);
                    close(server.repl_client->fd);
                    replica_client_destroy(server.repl_client);
                    server.repl_client = NULL;
                }
            } else {
                if(mask & EVENT_READABLE){
                    handle_client_read(fd, server.loop, &server.store, server.aof);
                }
                if(mask & EVENT_WRITABLE){
                    handle_client_write(fd, server.loop);
                }
            }
        }

        now_ms = monotonic_ms();
        if(now_ms >= next_flush_ms){
            if(aof_check_flush(server.aof) == AOF_OK){
                next_flush_ms = now_ms + 1000;
            }
        }

        // Periodic ACK from replica to primary
        if(server.repl_client && server.repl_client->state == REPL_CLIENT_STREAMING){
            if(now_ms - server.repl_client->last_ack_ms >= 1000){
                replica_client_send_ack(server.repl_client);
                event_loop_mod(server.loop, server.repl_client->fd,
                               EVENT_READABLE | EVENT_WRITABLE);
            }
        }

        // Advance PSYNC snapshot streaming and check snapshot fork children.
        // repl_check_sync_children returns the count of forks that exited this
        // tick so we can keep active_forks accurate and know when to resume
        // hashmap resizing and promote deferred PSYNC requests.
        int syncs_done = repl_check_sync_children(server.loop);
        if(syncs_done > 0){
            server.active_forks -= syncs_done;
            if(server.active_forks <= 0){
                server.active_forks = 0;
                hm_resume_resize(server.store.dict);
                // Start the next deferred PSYNC, one at a time to avoid
                // multiple concurrent forks hammering the store.
                for(int i = 0; i < server.replica_count; i++){
                    Replica *r = server.replicas[i];
                    if(r && r->state == REPLICA_PSYNC_PENDING){
                        launch_replica_sync(r, server.loop);
                        break;
                    }
                }
            }
        }
        repl_advance_snapshot_send(server.loop);

        check_pending_waits();

        // Clean up replicas that were marked dead (output overflow)
        for(int i = 0; i < server.replica_count; i++){
            Replica *r = server.replicas[i];
            if(r && r->dead){
                int dead_fd = r->fd;
                close_client(dead_fd, server.loop);
                i--;  // repl_remove_replica compacted the array
            }
        }

        // Check if AOF compaction child has finished
        if(server.aof->child_pid > 0){
            int status;
            pid_t pid = waitpid(server.aof->child_pid, &status, WNOHANG);
            if(pid > 0){
                if(!(WIFEXITED(status) && WEXITSTATUS(status) == 0)){
                    aof_recover_on_compact_fail(server.aof);
                    fprintf(stderr, "[AOF] compaction failed — recovered writes from tmp.aof\n");
                    server.aof->last_compaction_file_size = server.aof->file_size;
                    server.aof->child_pid = -1;
                    server.active_forks--;
                    if(server.active_forks <= 0){ server.active_forks = 0; hm_resume_resize(server.store.dict); }
                    continue;
                }

                aof_force_flush(server.aof);

                if(aof_merge_compacted(server.aof) != AOF_OK){
                    fprintf(stderr, "[AOF] compaction failed — merge error\n");
                    server.aof->child_pid = -1;
                    server.active_forks--;
                    if(server.active_forks <= 0){ server.active_forks = 0; hm_resume_resize(server.store.dict); }
                    continue;
                }

                rename("compacted.aof", "appendonly.aof");

                int new_fd = open("appendonly.aof", O_WRONLY | O_APPEND | O_DSYNC);
                int old_fd = server.aof->fd;
                aof_redirect(server.aof, new_fd);
                close(old_fd);
                unlink("tmp.aof");

                struct stat st;
                fstat(new_fd, &st);
                server.aof->file_size = st.st_size;
                server.aof->last_compaction_file_size = st.st_size;
                server.aof->child_pid = -1;
                server.active_forks--;
                if(server.active_forks <= 0){ server.active_forks = 0; hm_resume_resize(server.store.dict); }
                fprintf(stderr, "[AOF] compaction complete — new file size: %llu bytes\n",
                        (unsigned long long)server.aof->file_size);
            }
        } else if(aof_check_compact(server.aof) == AOF_OK){
            int aof_fd = open("tmp.aof", O_WRONLY | O_CREAT | O_APPEND | O_TRUNC | O_DSYNC, 0644);
            if(aof_fd == -1){
                perror("open tmp.aof");
            }

            int old_fd = server.aof->fd;
            aof_redirect(server.aof, aof_fd);
            close(old_fd);

            fprintf(stderr, "[AOF] compaction triggered — AOF at %llu bytes (%.0fx growth factor hit)\n",
                (unsigned long long)server.aof->file_size,
                server.aof->last_compaction_file_size > 0
                    ? (double)server.aof->file_size / (double)server.aof->last_compaction_file_size
                    : 0.0);

            hm_pause_resize(server.store.dict);
            server.active_forks++;

            pid_t pid = fork();
            if(pid == 0){
                aof_compact(&server.store);
            } else if(pid > 0){
                server.aof->child_pid = pid;
            } else {
                perror("[AOF] fork failed");
                server.aof->child_pid = -1;
                server.active_forks--;
                if(server.active_forks <= 0){ server.active_forks = 0; hm_resume_resize(server.store.dict); }
            }
        }
    }

    // Cleanup
    event_loop_destroy(server.loop);
    close(server.fd);

    for(int i = 0; i < MAX_CLIENT_FDS; i++){
        if(server.fd_clients[i]){
            ClientState *c = server.fd_clients[i];
            if(c->buffer){ free(c->buffer->data); free(c->buffer); }
            if(c->out_buffer){ free(c->out_buffer->data); free(c->out_buffer); }
            close(c->fd);
            free(c);
        }
    }

    return 0;
}

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if(flags == -1){
        return -1;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void handle_new_connection(int serverfd, event_loop_t *loop, RedisStore *store) {
    (void)store;

    struct sockaddr_storage client_addr;
    socklen_t sin_size = sizeof(client_addr);
    char s[INET6_ADDRSTRLEN];

    int clientfd = accept(serverfd, (struct sockaddr*)&client_addr, &sin_size);
    if(clientfd == -1){
        if(errno != EAGAIN && errno != EWOULDBLOCK){
            // perror("accept");
        }
        return;
    }

    if(set_nonblocking(clientfd) == -1){
        close(clientfd);
        return;
    }

    // Disable Nagle's algorithm — send responses immediately without waiting to
    // coalesce small packets. Prevents 40ms delayed-ACK stalls on real networks.
    int nodelay = 1;
    setsockopt(clientfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    ClientState *client = malloc(sizeof(ClientState));
    if(!client){
        close(clientfd);
        return;
    }

    client->fd = clientfd;
    client->event_mask = EVENT_READABLE;

    struct Buffer *buffer = malloc(sizeof(struct Buffer));
    if(!buffer){
        free(client);
        close(clientfd);
        return;
    }
    buffer->data = calloc(INITIAL_BUFF_CAPACITY, sizeof(char));
    if(!buffer->data){
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }
    buffer->capacity = INITIAL_BUFF_CAPACITY;
    buffer->max_capacity = MAX_CLIENT_INPUT_BUFF_LEN;
    buffer->used = 0;
    buffer->read_idx = 0;
    client->buffer = buffer;

    struct Buffer *out_buffer = malloc(sizeof(struct Buffer));
    if(!out_buffer){
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    out_buffer->data = calloc(INITIAL_OUT_BUFF_CAPACITY, sizeof(char));
    if(!out_buffer->data){
        free(out_buffer);
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    out_buffer->capacity = INITIAL_OUT_BUFF_CAPACITY;
    out_buffer->max_capacity = MAX_CLIENT_OUTPUT_BUFF_LEN;
    out_buffer->used = 0;
    out_buffer->read_idx = 0;
    client->out_buffer = out_buffer;
    client->output_overflow = 0;

    if(clientfd >= MAX_CLIENT_FDS){
        fprintf(stderr, "server: fd %d exceeds MAX_CLIENT_FDS — increase the limit\n", clientfd);
        free(out_buffer->data);
        free(out_buffer);
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    if(event_loop_add(loop, clientfd, EVENT_READABLE) == -1){
        free(out_buffer->data);
        free(out_buffer);
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    server.fd_clients[clientfd] = client;

    inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr*)&client_addr), s, sizeof(s));
    // printf("server: got connection from %s (fd=%d)\n", s, clientfd);
}

void handle_client_read(int clientfd, event_loop_t *loop, RedisStore *store, AOFManager *aof) {
    ClientState *client = (clientfd >= 0 && clientfd < MAX_CLIENT_FDS)
                          ? server.fd_clients[clientfd] : NULL;
    if(!client){
        close_client(clientfd, loop);
        return;
    }

    // Expose client to reply writer — avoids a hashmap lookup per write call
    server.current_client = client;

    struct Buffer *ib = client->buffer;

    // If all bytes from the previous recv batch were consumed, reset the buffer
    // positions to zero so the capacity check below never triggers a memmove for
    // an effectively-empty buffer. Mirrors the same reset in append_client_output.
    if(ib->read_idx == ib->used){
        ib->read_idx = 0;
        ib->used = 0;
    }

    if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
        size_t remaining = ib->used - ib->read_idx;
        memmove(ib->data, ib->data + ib->read_idx, remaining);
        ib->read_idx = 0;
        ib->used = remaining;

        if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
            if(!expand_buffer(ib)){
                sendError(clientfd, "Server error: failed to expand input buffer");
                flush_client_output(client);
                server.current_client = NULL;
                close_client(clientfd, loop);
                return;
            }
        }
    }

    ssize_t n = recv(clientfd, ib->data + ib->used, ib->capacity - ib->used, 0);

    if(n == 0){
        server.current_client = NULL;
        close_client(clientfd, loop);
        return;
    } else if(n < 0){
        if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR){
            server.current_client = NULL;
            return;
        }
        server.current_client = NULL;
        close_client(clientfd, loop);
        return;
    }

    ib->used += n;

    // Parse and execute all commands from this recv batch.
    // Responses accumulate in the output buffer; we flush once after the loop.
    while(ib->read_idx < ib->used){
        struct RedisCommand command;
        char *start_buff = ib->data + ib->read_idx;
        size_t buff_len = ib->used - ib->read_idx;
        ssize_t consumed = parse_array_command(start_buff, buff_len, &command);

        if(consumed == 0){
            if(ib->read_idx == 0 && ib->used == ib->capacity && ib->capacity >= ib->max_capacity){
                sendError(clientfd, "Protocol error: max input buffer length exceeded");
                flush_client_output(client);
                server.current_client = NULL;
                close_client(clientfd, loop);
                return;
            }
            break;
        } else if(consumed < 0){
            char *msg;
            switch(consumed){
                case ERR_INVALID_TYPE:    msg = "Protocol error: expected '*', got other"; break;
                case ERR_INVALID_ARRAY_L: msg = "Protocol error: invalid multibulk length"; break;
                case ERR_ARRAY_TOO_BIG:   msg = "Protocol error: too many arguments"; break;
                case ERR_INVALID_BULK_P:  msg = "Protocol error: expected '$', got other"; break;
                case ERR_INVALID_DELIM:   msg = "Protocol error: expected CRLF delimiter"; break;
                case ERR_BULK_TOO_BIG:    msg = "Protocol error: bulk string too long"; break;
                case ERR_MEM_ALLOC:       msg = "Server error: out of memory"; break;
                default:                  msg = "Protocol error: unknown error"; break;
            }
            sendError(clientfd, msg);
            flush_client_output(client);
            server.current_client = NULL;
            close_client(clientfd, loop);
            return;
        }

        ExecuteResult res = dispatch_command(clientfd, &command, store);

        if(res == EE_WRITE_OK){
            aof_add(aof, &command);
            repl_propagate(&command);
        } else if(res == EE_PSYNC_FULL_SYNC){
            handle_psync_full_sync(clientfd, loop);
        } else if(res == EE_WAIT_PENDING){
            long numreplicas = strtol(command.args[1].data, NULL, 10);
            long timeout_ms  = strtol(command.args[2].data, NULL, 10);
            register_pending_wait(clientfd, repl_get_offset(), (int)numreplicas, timeout_ms);
        }

        if(client->output_overflow){
            free_command(&command);
            server.current_client = NULL;
            close_client(clientfd, loop);
            return;
        }

        ib->read_idx += (size_t)consumed;
        free_command(&command);
    }

    server.current_client = NULL;

    // Arm writable events for STREAMING replicas that now have backlog data to send.
    // Replicas drain from the shared ring, not from out_buffer, so update_client_event_mask
    // (which only checks out_buffer) would never arm them — do it directly here.
    for(int i = 0; i < server.replica_count; i++){
        Replica *r = server.replicas[i];
        if(!r || r->state != REPLICA_STREAMING || r->dead) continue;
        if(r->fd >= 0 && r->fd < MAX_CLIENT_FDS){
            ClientState *rc = server.fd_clients[r->fd];
            if(rc && rc->event_mask != (EVENT_READABLE | EVENT_WRITABLE)){
                rc->event_mask = EVENT_READABLE | EVENT_WRITABLE;
                event_loop_mod(loop, r->fd, EVENT_READABLE | EVENT_WRITABLE);
            }
        }
    }

    // Flush all accumulated responses in one shot, then update the event mask once.
    ExecuteResult flush_res = flush_client_output(client);
    if(flush_res == EE_SOCK_CLOSED || flush_res == EE_ERR){
        close_client(clientfd, loop);
        return;
    }
    if(update_client_event_mask(client, loop) == -1){
        close_client(clientfd, loop);
    }
}

void handle_client_write(int clientfd, event_loop_t *loop) {
    ClientState *client = (clientfd >= 0 && clientfd < MAX_CLIENT_FDS)
                          ? server.fd_clients[clientfd] : NULL;
    if(!client) return;

    Replica *r = repl_find_replica(clientfd);
    if(r && r->state == REPLICA_STREAMING){
        // Drain any remaining out_buffer data first (e.g. the last snapshot chunk
        // that was in-flight when we transitioned from SENDING_SNAPSHOT).
        if(client->out_buffer && client->out_buffer->read_idx < client->out_buffer->used){
            ExecuteResult flush_res = flush_client_output(client);
            if(flush_res == EE_SOCK_CLOSED || flush_res == EE_ERR){
                close_client(clientfd, loop);
                return;
            }
            // Socket is full — come back on the next writable event.
            if(client->out_buffer->read_idx < client->out_buffer->used) return;
        }

        // out_buffer is empty — drain from the shared replication backlog.
        if(repl_backlog_drain_replica(r) < 0){
            close_client(clientfd, loop);
            return;
        }

        int mask = EVENT_READABLE;
        if(repl_backlog_has_data(r)) mask |= EVENT_WRITABLE;
        if(mask != client->event_mask){
            client->event_mask = mask;
            event_loop_mod(loop, clientfd, mask);
        }
        return;
    }

    // Normal client or snapshot-phase replica: drain out_buffer as before.
    ExecuteResult flush_res = flush_client_output(client);
    if(flush_res == EE_SOCK_CLOSED || flush_res == EE_ERR){
        close_client(clientfd, loop);
        return;
    }

    if(update_client_event_mask(client, loop) == -1){
        close_client(clientfd, loop);
    }
}

static ExecuteResult server_reply_write(int clientfd, const char *data, size_t len, void *ctx) {
    (void)ctx;
    if(clientfd == -1) return EE_OK;

    // server.current_client is set by handle_client_read before dispatch_command — no hashmap lookup needed
    ClientState *client = server.current_client;
    if(!client || client->output_overflow) return EE_ERR;

    if(!append_client_output(client, data, len)){
        client->output_overflow = 1;
        return EE_ERR;
    }
    return EE_OK;
}

static int append_client_output(ClientState *client, const char *data, size_t len) {
    if(len == 0){
        return 1;
    }

    struct Buffer *ob = client->out_buffer;
    if(!ob){
        return 0;
    }

    if(ob->read_idx == ob->used){
        ob->read_idx = 0;
        ob->used = 0;
    }

    if(ob->read_idx > 0 && (ob->capacity - ob->used) < len){
        size_t pending = ob->used - ob->read_idx;
        memmove(ob->data, ob->data + ob->read_idx, pending);
        ob->read_idx = 0;
        ob->used = pending;
    }

    size_t needed = ob->used + len;
    if(needed > ob->capacity){
        if(!expand_buffer_to(ob, needed)){
            return 0;
        }
    }

    memcpy(ob->data + ob->used, data, len);
    ob->used += len;
    return 1;
}

static ExecuteResult flush_client_output(ClientState *client) {
    if(!client || !client->out_buffer){
        return EE_ERR;
    }

    struct Buffer *ob = client->out_buffer;
    while(ob->read_idx < ob->used){
        size_t pending = ob->used - ob->read_idx;
        ssize_t n = send(client->fd, ob->data + ob->read_idx, pending, MSG_NOSIGNAL);
        if(n == -1){
            if(errno == EINTR) continue;
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                return EE_OK;
            }
            return EE_ERR;
        }
        if(n == 0){
            return EE_SOCK_CLOSED;
        }

        ob->read_idx += (size_t)n;
    }

    ob->read_idx = 0;
    ob->used = 0;
    return EE_OK;
}

static int update_client_event_mask(ClientState *client, event_loop_t *loop) {
    int mask = EVENT_READABLE;
    if(client->out_buffer && client->out_buffer->read_idx < client->out_buffer->used){
        mask |= EVENT_WRITABLE;
    }
    // Skip the event_loop_mod syscall when the mask hasn't changed
    if(mask == client->event_mask) return 0;
    client->event_mask = mask;
    return event_loop_mod(loop, client->fd, mask);
}

void close_client(int clientfd, event_loop_t *loop) {
    repl_remove_replica(clientfd);  // no-op if fd is not a replica
    cancel_pending_waits(clientfd);
    event_loop_del(loop, clientfd, EVENT_READABLE | EVENT_WRITABLE);

    if(clientfd < 0 || clientfd >= MAX_CLIENT_FDS) return;
    ClientState *client = server.fd_clients[clientfd];
    server.fd_clients[clientfd] = NULL;

    if(client){
        if(client->buffer){
            free(client->buffer->data);
            free(client->buffer);
        }
        if(client->out_buffer){
            free(client->out_buffer->data);
            free(client->out_buffer);
        }
        close(client->fd);
        free(client);
    }
}

void *get_in_addr(struct sockaddr *sa){
    if(sa->sa_family == AF_INET){
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }
    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

static int any_fork_running(void) {
    return server.active_forks > 0;
}

// Fork the snapshot for a replica that is ready to sync.  Can be called
// immediately (when no fork is running) or deferred (after a prior fork
// finishes and REPLICA_PSYNC_PENDING replicas are promoted).
static void launch_replica_sync(Replica *r, event_loop_t *loop) {
    (void)loop;
    snprintf(r->sync_file, sizeof(r->sync_file), "fullsync_%d.aof", r->fd);

    hm_pause_resize(server.store.dict);
    server.active_forks++;

    pid_t pid = fork();
    if(pid == 0){
        aof_compact_to_file(&server.store, r->sync_file);
        // aof_compact_to_file calls _exit() — never returns
    } else if(pid > 0){
        r->sync_child_pid = pid;
        r->state = REPLICA_FULL_SYNC;
        fprintf(stderr, "[repl] full sync started for fd=%d (child pid=%d)\n", r->fd, pid);
    } else {
        perror("[repl] fork failed");
        server.active_forks--;
        if(server.active_forks <= 0){ server.active_forks = 0; hm_resume_resize(server.store.dict); }
        r->dead = 1;
    }
}

// Called from handle_client_read when exec_psync returns EE_PSYNC_FULL_SYNC.
// If another fork is already running we park the replica in PSYNC_PENDING
// and launch the snapshot once that fork finishes.
static void handle_psync_full_sync(int clientfd, event_loop_t *loop) {
    ClientState *client = (clientfd >= 0 && clientfd < MAX_CLIENT_FDS)
                          ? server.fd_clients[clientfd] : NULL;
    if(!client){ close_client(clientfd, loop); return; }

    if(repl_add_replica(clientfd, client->out_buffer, server.repl_offset) != 0){
        fprintf(stderr, "[repl] repl_add_replica failed\n");
        close_client(clientfd, loop);
        return;
    }

    Replica *r = repl_find_replica(clientfd);
    if(!r){ close_client(clientfd, loop); return; }

    if(any_fork_running()){
        // Park the replica — writes accumulate in sync_buf until the fork
        // completes and the event loop promotes it.
        r->state = REPLICA_PSYNC_PENDING;
        fprintf(stderr, "[repl] PSYNC fd=%d: deferred (fork already running)\n", clientfd);
        return;
    }

    launch_replica_sync(r, loop);
}

// Park a WAIT that could not be satisfied immediately.
static void register_pending_wait(int clientfd, uint64_t target, int numreplicas, long timeout_ms) {
    for (int i = 0; i < MAX_PENDING_WAITS; i++) {
        if (!server.pending_waits[i].active) {
            server.pending_waits[i].clientfd      = clientfd;
            server.pending_waits[i].target_offset = target;
            server.pending_waits[i].deadline_ms   = monotonic_ms() + (uint64_t)timeout_ms;
            server.pending_waits[i].numreplicas   = numreplicas;
            server.pending_waits[i].active        = 1;
            return;
        }
    }
    // Table full — satisfy immediately with current count so the client isn't
    // left hanging forever.
    ClientState *c = server.fd_clients[clientfd];
    if (c) {
        server.current_client = c;
        sendInt(clientfd, repl_count_synced(target));
        server.current_client = NULL;
        flush_client_output(c);
        update_client_event_mask(c, server.loop);
    }
}

// Called every event loop tick.  Resolves any parked WAIT whose condition is
// now met (enough replicas ACK'd) or whose deadline has passed.
static void check_pending_waits(void) {
    uint64_t now = monotonic_ms();
    for (int i = 0; i < MAX_PENDING_WAITS; i++) {
        PendingWait *pw = &server.pending_waits[i];
        if (!pw->active) continue;

        int synced = repl_count_synced(pw->target_offset);
        int satisfied = (synced >= pw->numreplicas) || (now >= pw->deadline_ms);
        if (!satisfied) continue;

        pw->active = 0;

        // Guard against the client having disconnected while waiting.
        if (pw->clientfd < 0 || pw->clientfd >= MAX_CLIENT_FDS) continue;
        ClientState *c = server.fd_clients[pw->clientfd];
        if (!c) continue;

        server.current_client = c;
        sendInt(pw->clientfd, synced);
        server.current_client = NULL;
        flush_client_output(c);
        update_client_event_mask(c, server.loop);
    }
}

// Returns the earliest deadline across all active pending WAITs, or UINT64_MAX
// when none exist.  Used to cap the event_loop_wait timeout.
static uint64_t next_pending_wait_deadline_ms(void) {
    uint64_t earliest = UINT64_MAX;
    for (int i = 0; i < MAX_PENDING_WAITS; i++) {
        if (server.pending_waits[i].active &&
            server.pending_waits[i].deadline_ms < earliest) {
            earliest = server.pending_waits[i].deadline_ms;
        }
    }
    return earliest;
}

// Drop all pending WAITs for a client that is being closed.
static void cancel_pending_waits(int clientfd) {
    for (int i = 0; i < MAX_PENDING_WAITS; i++) {
        if (server.pending_waits[i].active && server.pending_waits[i].clientfd == clientfd) {
            server.pending_waits[i].active = 0;
        }
    }
}

// Establish a non-blocking TCP connection to the primary and create the replica context.
static int connect_to_primary(const char *host, const char *port, uint16_t our_port) {
    struct addrinfo hints, *res, *p;
    memset(&hints, 0, sizeof hints);
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if(getaddrinfo(host, port, &hints, &res) != 0){
        fprintf(stderr, "[replica] getaddrinfo failed for %s:%s\n", host, port);
        return -1;
    }

    int fd = -1;
    for(p = res; p; p = p->ai_next){
        fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if(fd == -1) continue;
        set_nonblocking(fd);
        int nd = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
        int rv = connect(fd, p->ai_addr, p->ai_addrlen);
        if(rv == 0 || (rv == -1 && errno == EINPROGRESS)) break;
        close(fd); fd = -1;
    }
    freeaddrinfo(res);

    if(fd == -1){
        fprintf(stderr, "[replica] could not connect to %s:%s\n", host, port);
        return -1;
    }

    server.repl_client = replica_client_create(fd, our_port);
    if(!server.repl_client){ close(fd); return -1; }

    if(event_loop_add(server.loop, fd, EVENT_WRITABLE) == -1){
        replica_client_destroy(server.repl_client);
        server.repl_client = NULL;
        close(fd);
        return -1;
    }

    printf("[replica] connecting to primary %s:%s\n", host, port);
    return 0;
}
