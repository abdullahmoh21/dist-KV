#include "store/buffer.h"
#include "store/skip_list.h"
#include "store/hashmap.h"
#include "store/redis_store.h"
#include "engine/execution_engine.h"
#include "parser/resp_parser.h"
#include "event_loop/event_loop.h"
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

#define PORT "6379"
#define BACKLOG 10
#define EXPANSION_THRESH 10
#define MAX_EVENTS 128

// Client connection state
typedef struct {
    int fd;
    struct Buffer *buffer;
} ClientState;

// Prototypes
int set_nonblocking(int fd);
const KeyView get_client_key(void *client_ptr);
void handle_new_connection(int serverfd, event_loop_t *loop, HashMap *clients, RedisStore *store);
void handle_client_read(int clientfd, event_loop_t *loop, HashMap *clients, RedisStore *store);
void close_client(int clientfd, event_loop_t *loop, HashMap *clients);
void *get_in_addr(struct sockaddr *sa);

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

    // Initialize client registry using existing HashMap
    HashMap *clients = hm_create(get_client_key);
    if(!clients){
        fprintf(stderr, "Failed to create client registry\n");
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
            perror("server: socket");
            continue;
        }

        if (setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &yes,
            sizeof(int)) == -1) {
            perror("setsockopt");
            exit(1);
        }

        if(bind(serverfd, p->ai_addr, p->ai_addrlen) == -1){
            close(serverfd);
            perror("server: bind");
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
        perror("listen");
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
    while(1) {
        int n = event_loop_wait(loop, -1, events, MAX_EVENTS);
        
        if(n < 0){
            if(errno == EINTR) continue;
            perror("event_loop_wait");
            break;
        }

        for(int i = 0; i < n; i++){
            int fd = events[i].fd;
            int mask = events[i].mask;

            if(fd == serverfd){
                // New connection
                handle_new_connection(serverfd, loop, clients, &store);
            } else if(mask & EVENT_READABLE){
                // Data available on client socket
                handle_client_read(fd, loop, clients, &store);
            }
        }
    }

    // Cleanup
    event_loop_destroy(loop);
    close(serverfd);
    
    // Note: In production, iterate through HashMap and free all ClientStates
    // For now, we rely on process termination to clean up
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
const KeyView get_client_key(void *client_ptr) {
    ClientState *client = (ClientState *)client_ptr;
    KeyView kv;
    kv.data = (const char *)&client->fd;
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
            perror("accept");
        }
        return;
    }

    // Set client socket to non-blocking
    if(set_nonblocking(clientfd) == -1){
        perror("set_nonblocking");
        close(clientfd);
        return;
    }

    // Allocate client state
    ClientState *client = malloc(sizeof(ClientState));
    if(!client){
        perror("malloc client");
        close(clientfd);
        return;
    }
    
    client->fd = clientfd;

    // Initialize buffer for this client
    struct Buffer *buffer = malloc(sizeof(struct Buffer));
    if(!buffer){
        perror("malloc buffer");
        free(client);
        close(clientfd);
        return;
    }
    buffer->data = calloc(INITIAL_BUFF_CAPACITY, sizeof(char));
    if(!buffer->data){
        perror("calloc buffer data");
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }
    buffer->capacity = INITIAL_BUFF_CAPACITY;
    buffer->used = 0;
    buffer->read_idx = 0;
    client->buffer = buffer;

    // Add to event loop
    if(event_loop_add(loop, clientfd, EVENT_READABLE) == -1){
        perror("event_loop_add");
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    // Add to client HashMap
    if(hm_insert(clients, client) != HM_OK){
        fprintf(stderr, "Failed to insert client into registry\n");
        event_loop_del(loop, clientfd, EVENT_READABLE);
        free(buffer->data);
        free(buffer);
        free(client);
        close(clientfd);
        return;
    }

    inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr*)&client_addr), s, sizeof(s));
    printf("server: got connection from %s (fd=%d, total clients=%zu)\n", s, clientfd, clients->item_count);
}

void handle_client_read(int clientfd, event_loop_t *loop, HashMap *clients, RedisStore *store) {
    // Find client state using HashMap
    ClientState *client = NULL;
    if(hm_get(clients, (const char *)&clientfd, sizeof(int), (void **)&client) != HM_OK || !client){
        fprintf(stderr, "Client not found for fd %d\n", clientfd);
        close_client(clientfd, loop, clients);
        return;
    }

    struct Buffer *ib = client->buffer;

    // Ensure buffer has space
    if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
        // Try removing used data
        size_t remaining = ib->used - ib->read_idx;
        memmove(ib->data, ib->data + ib->read_idx, remaining);    
        ib->read_idx = 0;
        ib->used = remaining;
        
        // Still need more -> expand buffer
        if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
            if(!expand_buffer(ib)){
                fprintf(stderr, "[ERROR] Failed to expand buffer\n");
                close_client(clientfd, loop, clients);
                return;
            }
        }
    }

    // Read data from socket
    ssize_t n = recv(clientfd, ib->data + ib->used, ib->capacity - ib->used, 0);
    
    if(n == 0) {
        // Client closed connection
        printf("Client fd=%d closed connection\n", clientfd);
        close_client(clientfd, loop, clients);
        return;
    } else if (n < 0) {
        if(errno == EAGAIN || errno == EWOULDBLOCK){
            // No more data available right now
            return;
        }
        if(errno == EINTR){
            return;
        }
        perror("recv");
        close_client(clientfd, loop, clients);
        return;
    }

    ib->used += n;
  
    // Parse and execute commands
    while(ib->read_idx < ib->used){
        struct RedisCommand command;
        char *start_buff = ib->data + ib->read_idx;
        size_t buff_len = ib->used - ib->read_idx;
        ssize_t consumed = parse_array_command(start_buff, buff_len, &command);

        if(consumed == 0){
            // Need more data
            break;
        } else if(consumed < 0){
            fprintf(stderr, "[ERROR] Parse error: %zd\n", consumed);
            char *msg;
            switch (consumed) {
                case ERR_INVALID_TYPE:    msg = "Protocol error: expected '*', got other"; break;
                case ERR_INVALID_ARRAY_L: msg = "Protocol error: invalid multibulk length"; break;
                case ERR_ARRAY_TOO_BIG:   msg = "Protocol error: too many arguments"; break;
                case ERR_INVALID_BULK_P:  msg = "Protocol error: expected '$', got other"; break;
                case ERR_BULK_TOO_BIG:    msg = "Protocol error: bulk string too long"; break;
                case ERR_MEM_ALLOC:       msg = "Server error: out of memory"; break;
                default:                  msg = "Protocol error: unknown error"; break;
            }
            sendError(clientfd, msg);
            close_client(clientfd, loop, clients);
            return;
        }
        
        ExecuteResult res = dispatch_command(clientfd, &command, store);
        (void)res; // Unused for now
        ib->read_idx += (size_t) consumed;
        free_command(&command);
    }
}

void close_client(int clientfd, event_loop_t *loop, HashMap *clients) {
    // Remove from event loop
    event_loop_del(loop, clientfd, EVENT_READABLE | EVENT_WRITABLE);
    
    // Remove from HashMap and cleanup
    ClientState *client = NULL;
    if(hm_delete(clients, (const char *)&clientfd, sizeof(int), (void **)&client) == HM_OK && client){
        if(client->buffer){
            free(client->buffer->data);
            free(client->buffer);
        }
        close(client->fd);
        free(client);
        printf("Closed client fd=%d (remaining clients=%zu)\n", clientfd, clients->item_count);
    }
}

void *get_in_addr(struct sockaddr *sa){
 if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}
