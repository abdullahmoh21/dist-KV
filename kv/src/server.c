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

#define PORT "6379"
#define BACKLOG 511                        // matches Redis default; avoids SYN drops under burst load
#define EXPANSION_THRESH 10
#define MAX_EVENTS 128
#define INITIAL_OUT_BUFF_CAPACITY 4096
#define MAX_CLIENT_OUTPUT_BUFF_LEN (64 * 1024 * 1024)
// Direct fd-indexed client table: O(1) lookup by file descriptor, no hashing.
// Replaces the HashMap used previously for hot-path client lookups.
// Size must exceed the process's open-file limit; 4096 is safe for typical deployments.
#define MAX_CLIENT_FDS 4096

// Client connection state
typedef struct {
    int fd;
    int event_mask;         
    struct Buffer *buffer;
    struct Buffer *out_buffer;
    int output_overflow;
} ClientState;

// Server-wide state — single instance, file-static
typedef struct {
    int fd;
    event_loop_t *loop;
    ClientState *fd_clients[MAX_CLIENT_FDS]; // direct fd→client mapping, O(1) hot-path lookup
    RedisStore store;
    AOFManager *aof;
    ClientState *current_client;
    Replica **replicas;
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

int main(void){
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

    if((rv = getaddrinfo(NULL, PORT, &hints, &servinfo)) != 0){
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

    printf("server: waiting for connections on port %s\n", PORT);

    // Main event loop
    fired_event_t events[MAX_EVENTS];
    uint64_t next_flush_ms = monotonic_ms() + 1000;
    while(1) {
        uint64_t now_ms = monotonic_ms();
        int timeout_ms = (next_flush_ms > now_ms) ? ((int)(next_flush_ms - now_ms)) : 0;
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
                    hm_resume_resize(server.store.dict);
                    continue;
                }

                aof_force_flush(server.aof);

                if(aof_merge_compacted(server.aof) != AOF_OK){
                    fprintf(stderr, "[AOF] compaction failed — merge error\n");
                    server.aof->child_pid = -1;
                    hm_resume_resize(server.store.dict);
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
                hm_resume_resize(server.store.dict);
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

            pid_t pid = fork();
            if(pid == 0){
                aof_compact(&server.store);
            } else if(pid > 0){
                server.aof->child_pid = pid;
            } else {
                perror("[AOF] fork failed");
                hm_resume_resize(server.store.dict);
                server.aof->child_pid = -1;
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
