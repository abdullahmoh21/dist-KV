#include "store/buffer.h"
#include "aof/aof.h"
#include "store/skip_list.h"
#include "store/hashmap.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "parser/resp_parser.h"
#include "event_loop/event_loop.h"
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
#define BACKLOG 10
#define EXPANSION_THRESH 10
#define MAX_EVENTS 128
#define INITIAL_OUT_BUFF_CAPACITY 4096
#define MAX_CLIENT_OUTPUT_BUFF_LEN (64 * 1024 * 1024)

// Client connection state
typedef struct {
    int fd;
    int event_mask;         // cached event mask — avoids redundant event_loop_mod syscalls
    struct Buffer *buffer;
    struct Buffer *out_buffer;
    int output_overflow;
} ClientState;

// Set before dispatch_command so reply writer can find the client without a hashmap lookup
static ClientState *g_current_client = NULL;

// Prototypes
int set_nonblocking(int fd);
KeyView get_client_key(void *client_ptr);
void handle_new_connection(int serverfd, event_loop_t *loop, HashMap *clients, RedisStore *store);
void handle_client_read(int clientfd, event_loop_t *loop, HashMap *clients, RedisStore *store, AOFManager *aof);
void handle_client_write(int clientfd, event_loop_t *loop, HashMap *clients);
void close_client(int clientfd, event_loop_t *loop, HashMap *clients);
void *get_in_addr(struct sockaddr *sa);
static ExecuteResult server_reply_write(int clientfd, const char *data, size_t len, void *ctx);
static int append_client_output(ClientState *client, const char *data, size_t len);
static ExecuteResult flush_client_output(ClientState *client);
static int update_client_event_mask(ClientState *client, event_loop_t *loop);

int main(void){
    int serverfd;
    struct addrinfo hints, *servinfo, *p;
    int yes=1;
    int rv;

    // Initialize Redis store
    RedisStore store;
    if(create_store(&store) != RS_OK){
        fprintf(stderr, "Failed to create Redis store\n");
        return 1;
    }

    enum AOF_RESULT res = aof_load(&store);
    if(res != AOF_OK){
        fprintf(stderr, "Failed to load AOF into memory\n");
        return 1;
    }
    

    HashMap *clients = hm_create(get_client_key);
    if(!clients){
        fprintf(stderr, "Failed to create client registry\n");
        return 1;
    }

    ee_set_reply_writer(server_reply_write, clients);

    AOFManager *aof;
    if(aof_create(&aof) != AOF_OK){
        fprintf(stderr, "Failed to create AOF buffer\n");
        return 1;
    }


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
        serverfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if(serverfd == -1){
            // perror("server: socket");
            continue;
        }

        if (setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &yes,
            sizeof(int)) == -1) {
            // perror("setsockopt");
            exit(1);
        }

        if(bind(serverfd, p->ai_addr, p->ai_addrlen) == -1){
            close(serverfd);
            // perror("server: bind");
            continue;
        }
        break;
    }
    
    freeaddrinfo(servinfo);

    if(p == NULL){
        fprintf(stderr, "server: failed to bind\n");
        exit(1);
    }

    if(listen(serverfd, BACKLOG) == -1){
        // perror("listen");
        exit(1);
    }

    // Set server socket to non-blocking
    if(set_nonblocking(serverfd) == -1){
        fprintf(stderr, "Failed to set server socket non-blocking\n");
        close(serverfd);
        return 1;
    }

    // Create event loop
    event_loop_t *loop = event_loop_create(MAX_EVENTS);
    if(!loop){
        fprintf(stderr, "Failed to create event loop\n");
        close(serverfd);
        return 1;
    }

    // Add server socket to event loop
    if(event_loop_add(loop, serverfd, EVENT_READABLE) == -1){
        fprintf(stderr, "Failed to add server socket to event loop\n");
        event_loop_destroy(loop);
        close(serverfd);
        return 1;
    }

    printf("server: waiting for connections on port %s\n", PORT);

    // Main event loop
    fired_event_t events[MAX_EVENTS];
    uint64_t next_flush_ms = monotonic_ms() + 1000;
    while(1) {
        uint64_t now_ms = monotonic_ms();
        int timeout_ms = (next_flush_ms > now_ms) ? ((int) next_flush_ms - now_ms) : 0;
        int n = event_loop_wait(loop, timeout_ms, events, MAX_EVENTS);
        
        if(n < 0){
            if(errno == EINTR) continue;
            // perror("event_loop_wait");
            break;
        }

        for(int i = 0; i < n; i++){
            int fd = events[i].fd;
            int mask = events[i].mask;

            if(fd == serverfd){
                // New connection
                handle_new_connection(serverfd, loop, clients, &store);
            } else {
                if(mask & EVENT_READABLE){
                    // Data available on client socket
                    handle_client_read(fd, loop, clients, &store, aof);
                }
                if(mask & EVENT_WRITABLE){
                    // Pending response data can be flushed
                    handle_client_write(fd, loop, clients);
                }
            }
        }

        now_ms = monotonic_ms();
        if(now_ms >= next_flush_ms){
            if(aof_check_flush(aof) == AOF_OK){
                next_flush_ms = now_ms + 1000;
            }
        }
        
        // check if we need to spawn aof_compact process
        if(aof->child_pid > 0){
            int status;
            pid_t pid = waitpid(aof->child_pid, &status, WNOHANG);
            if(pid > 0){
                if(!(WIFEXITED(status) && WEXITSTATUS(status) == 0)){
                    fprintf(stderr, "[AOF] compaction child %d failed (status=%d), recovering tmp.aof -> appendonly.aof\n", aof->child_pid, status);
                    if(aof_recover_on_compact_fail(aof) != AOF_OK){
                        fprintf(stderr, "[AOF] recovery failed — writes in tmp.aof may be lost\n");
                    } else {
                        fprintf(stderr, "[AOF] recovery ok, file_size=%llu\n", (unsigned long long)aof->file_size);
                    }
                    aof->last_compaction_file_size = aof->file_size;
                    aof->child_pid = -1;
                    hm_resume_resize(store.dict);
                    continue;
                }

                fprintf(stderr, "[AOF] compaction child done, merging tmp.aof -> compacted.aof\n");
                aof_force_flush(aof);

                if(aof_merge_compacted(aof) != AOF_OK){
                    fprintf(stderr, "[AOF] merge failed\n");
                    aof->child_pid = -1;
                    hm_resume_resize(store.dict);
                    continue;
                }

                rename("compacted.aof", "appendonly.aof");

                int new_fd = open("appendonly.aof", O_WRONLY | O_APPEND);
                int old_fd = aof->fd;
                aof_redirect(aof, new_fd);
                close(old_fd);
                unlink("tmp.aof");

                struct stat st;
                fstat(new_fd, &st);
                aof->file_size = st.st_size;
                aof->last_compaction_file_size = st.st_size;
                aof->child_pid = -1;
                hm_resume_resize(store.dict);
                fprintf(stderr, "[AOF] compaction complete, new file_size=%llu bytes\n", (unsigned long long)st.st_size);
            }
        } else if(aof_check_compact(aof) == AOF_OK){
            int aof_fd = open("tmp.aof", O_WRONLY | O_CREAT | O_APPEND | O_TRUNC, 0644);
            if (aof_fd == -1) {
                perror("open tmp.aof");
            }

            int old_fd = aof->fd;
            aof_redirect(aof, aof_fd);
            close(old_fd);

            fprintf(stderr, "[AOF] triggering compaction: file_size=%llu last_compaction=%llu\n",
                (unsigned long long)aof->file_size,
                (unsigned long long)aof->last_compaction_file_size);

            hm_pause_resize(store.dict);

            pid_t pid = fork();
            
            if(pid == 0){
                aof_compact(&store);
            } else if(pid > 0){
                aof->child_pid = pid;
                fprintf(stderr, "[AOF] compaction child spawned pid=%d\n", pid);
            } else {
                perror("[AOF] fork failed");
                hm_resume_resize(store.dict);
                aof->child_pid = -1;
            }
        }
    }

    // Cleanup
    event_loop_destroy(loop);
    close(serverfd);
    
    // TODO: iterate through HashMap and free all ClientStates
    hm_free_shallow(clients);

    return 0;
}

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return -1;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Key extraction function for HashMap
KeyView get_client_key(void *client_ptr) {
    ClientState *client = (ClientState *)client_ptr;
    KeyView kv;
    kv.data = (char *)&client->fd;
    kv.len = sizeof(int);
    return kv;
}

void handle_new_connection(int serverfd, event_loop_t *loop, HashMap *clients, RedisStore *store) {
    (void)store; // Unused for now
    
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

    // Set client socket to non-blocking
    if(set_nonblocking(clientfd) == -1){
        // perror("set_nonblocking");
        close(clientfd);
        return;
    }

    // Disable Nagle's algorithm — send responses immediately without waiting to
    // coalesce small packets. Prevents 40ms delayed-ACK stalls on real networks.
    int nodelay = 1;
    setsockopt(clientfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    // Allocate client state
    ClientState *client = malloc(sizeof(ClientState));
    if(!client){
        // perror("malloc client");
        close(clientfd);
        return;
    }
    
    client->fd = clientfd;
    client->event_mask = EVENT_READABLE;

    // Initialize buffer for this client
    struct Buffer *buffer = malloc(sizeof(struct Buffer));
    if(!buffer){
        // perror("malloc buffer");
        free(client);
        close(clientfd);
        return;
    }
    buffer->data = calloc(INITIAL_BUFF_CAPACITY, sizeof(char));
    if(!buffer->data){
        // perror("calloc buffer data");
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

    // Add to event loop
    if(event_loop_add(loop, clientfd, EVENT_READABLE) == -1){
        // perror("event_loop_add");
        free(out_buffer->data);
        free(out_buffer);
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    // Add to client HashMap
    if(hm_insert(clients, client) != HM_OK){
        // fprintf(stderr, "Failed to insert client into registry\n");
        event_loop_del(loop, clientfd, EVENT_READABLE);
        free(out_buffer->data);
        free(out_buffer);
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr*)&client_addr), s, sizeof(s));
    // printf("server: got connection from %s (fd=%d, total clients=%zu)\n", s, clientfd, clients->item_count);
}

void handle_client_read(int clientfd, event_loop_t *loop, HashMap *clients, RedisStore *store, AOFManager *aof) {
    ClientState *client = NULL;
    if(hm_get(clients, (char *)&clientfd, sizeof(int), (void **)&client) != HM_OK || !client){
        close_client(clientfd, loop, clients);
        return;
    }

    // Expose client to reply writer — avoids a hashmap lookup per write call
    g_current_client = client;

    struct Buffer *ib = client->buffer;

    if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
        // Try removing used data
        size_t remaining = ib->used - ib->read_idx;
        memmove(ib->data, ib->data + ib->read_idx, remaining);
        ib->read_idx = 0;
        ib->used = remaining;

        // Still need more -> expand buffer
        if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
            if(!expand_buffer(ib)){
                sendError(clientfd, "Server error: failed to expand input buffer");
                flush_client_output(client);
                g_current_client = NULL;
                close_client(clientfd, loop, clients);
                return;
            }
        }
    }

    ssize_t n = recv(clientfd, ib->data + ib->used, ib->capacity - ib->used, 0);

    if(n == 0) {
        g_current_client = NULL;
        close_client(clientfd, loop, clients);
        return;
    } else if (n < 0) {
        if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR){
            g_current_client = NULL;
            return;
        }
        g_current_client = NULL;
        close_client(clientfd, loop, clients);
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
            if (ib->read_idx == 0 && ib->used == ib->capacity && ib->capacity >= ib->max_capacity) {
                sendError(clientfd, "Protocol error: max input buffer length exceeded");
                flush_client_output(client);
                g_current_client = NULL;
                close_client(clientfd, loop, clients);
                return;
            }
            // Need more data
            break;
        } else if(consumed < 0){
            char *msg;
            switch (consumed) {
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
            g_current_client = NULL;
            close_client(clientfd, loop, clients);
            return;
        }

        ExecuteResult res = dispatch_command(clientfd, &command, store);

        if(res == EE_WRITE_OK){
            aof_add(aof, &command);
        }

        if (client->output_overflow) {
            free_command(&command);
            g_current_client = NULL;
            close_client(clientfd, loop, clients);
            return;
        }

        ib->read_idx += (size_t) consumed;
        free_command(&command);
    }

    g_current_client = NULL;

    // Flush all accumulated responses in one shot, then update the event mask once.
    ExecuteResult flush_res = flush_client_output(client);
    if (flush_res == EE_SOCK_CLOSED || flush_res == EE_ERR) {
        close_client(clientfd, loop, clients);
        return;
    }
    if (update_client_event_mask(client, loop) == -1) {
        close_client(clientfd, loop, clients);
    }
}

void handle_client_write(int clientfd, event_loop_t *loop, HashMap *clients) {
    ClientState *client = NULL;
    if (hm_get(clients, (char *)&clientfd, sizeof(int), (void **)&client) != HM_OK || !client) {
        return;
    }

    ExecuteResult flush_res = flush_client_output(client);
    if (flush_res == EE_SOCK_CLOSED || flush_res == EE_ERR) {
        close_client(clientfd, loop, clients);
        return;
    }

    if (update_client_event_mask(client, loop) == -1) {
        close_client(clientfd, loop, clients);
    }
}

static ExecuteResult server_reply_write(int clientfd, const char *data, size_t len, void *ctx) {
    (void)ctx;
    if (clientfd == -1) return EE_OK;

    // g_current_client is set by handle_client_read before dispatch_command — no hashmap lookup needed
    ClientState *client = g_current_client;
    if (!client || client->output_overflow) return EE_ERR;

    if (!append_client_output(client, data, len)) {
        client->output_overflow = 1;
        return EE_ERR;
    }
    return EE_OK;
}

static int append_client_output(ClientState *client, const char *data, size_t len) {
    if (len == 0) {
        return 1;
    }

    struct Buffer *ob = client->out_buffer;
    if (!ob) {
        return 0;
    }

    if (ob->read_idx == ob->used) {
        ob->read_idx = 0;
        ob->used = 0;
    }

    if (ob->read_idx > 0 && (ob->capacity - ob->used) < len) {
        size_t pending = ob->used - ob->read_idx;
        memmove(ob->data, ob->data + ob->read_idx, pending);
        ob->read_idx = 0;
        ob->used = pending;
    }

    size_t needed = ob->used + len;
    if (needed > ob->capacity) {
        if (!expand_buffer_to(ob, needed)) {
            return 0;
        }
    }

    memcpy(ob->data + ob->used, data, len);
    ob->used += len;
    return 1;
}

static ExecuteResult flush_client_output(ClientState *client) {
    if (!client || !client->out_buffer) {
        return EE_ERR;
    }

    struct Buffer *ob = client->out_buffer;
    while (ob->read_idx < ob->used) {
        size_t pending = ob->used - ob->read_idx;
        ssize_t n = send(client->fd, ob->data + ob->read_idx, pending, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return EE_OK;
            }
            return EE_ERR;
        }
        if (n == 0) {
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
    if (client->out_buffer && client->out_buffer->read_idx < client->out_buffer->used) {
        mask |= EVENT_WRITABLE;
    }
    // Skip the event_loop_mod syscall when the mask hasn't changed
    if (mask == client->event_mask) return 0;
    client->event_mask = mask;
    return event_loop_mod(loop, client->fd, mask);
}

void close_client(int clientfd, event_loop_t *loop, HashMap *clients) {
    // Remove from event loop
    event_loop_del(loop, clientfd, EVENT_READABLE | EVENT_WRITABLE);
    
    // Remove from HashMap and cleanup
    ClientState *client = NULL;
    if(hm_delete(clients, (char *)&clientfd, sizeof(int), (void **)&client) == HM_OK && client){
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
        // printf("Closed client fd=%d (remaining clients=%zu)\n", clientfd, clients->item_count);
    }
}

void *get_in_addr(struct sockaddr *sa){
 if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

