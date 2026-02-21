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
#include <buffer.h>

#define PORT "6379"
#define BACKLOG 10
#define INITIAL_BUFF_CAPACITY 4096
#define EXPANSION_THRESH 10

enum ParseResult { PARSE_OK, PARSE_NEED_MORE, PARSE_ERR };
enum ExecuteStatus { EXECUTE_OK, EXECUTE_ERR };
enum CommandType { REDIS_GET, REDIS_PUT};
struct RedisCommand {
    enum CommandType type;
    struct BulkString *args;
    size_t arg_count;
};

struct BulkString{
    void *data;
    size_t len;
};

void *get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int main(void){
    int serverfd, clientfd;
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage client_addr;
    socklen_t sin_size;
    int yes=1;
    char s[INET6_ADDRSTRLEN];
    struct InputBuffer *ib = malloc(sizeof(struct InputBuffer));
    ib->data = calloc(INITIAL_BUFF_CAPACITY, sizeof(char));
    ib->capacity = INITIAL_BUFF_CAPACITY;
    ib->used = 0;
    int rv;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if(getaddrinfo(NULL, PORT, &hints, &servinfo) != 0){
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

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

    printf("server: waiting for connections on port %s\n", PORT);

    while(1) {  // listening loop
        sin_size = sizeof client_addr;
        clientfd = accept(serverfd, (struct sockaddr*) &client_addr, &sin_size);

        if(clientfd == -1){
            perror("accept");
            continue;
        }

        inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr *) &client_addr), s, sizeof s);
        printf("server: got connection from %s\n", s);

        while(1){
            if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
                // try removing used data
                size_t remaining = ib->used - ib->read_idx;
                memmove(ib->data, ib->data + ib->read_idx, remaining);    
                ib->read_idx = 0;
                ib->used = remaining;
                // still need more -> expand buffer
                if(ib->capacity - ib->used <= (ib->capacity * EXPANSION_THRESH) / 100){
                    if(!expand_buffer(ib)){
                        printf("Error expanding buffer");
                        exit(1);
                    }
                }
            }

            ssize_t n = recv(clientfd, ib->data + ib->used, ib->capacity - ib->used, 0);
            if(n == 0) {    // client closed
                break;
            } else if (n < 0) { // error
                if(errno == EINTR){continue;}
                perror("recv");
                break;
            }

            ib->used += n;
          
            while(ib->read_idx < ib->used){
                if(ib->data[ib->read_idx] == '*'){   
                    struct RedisCommand command;
                    char *start_buff = ib->data + ib->read_idx;
                    size_t buff_len = ib->used - ib->read_idx;
                    int consumed = parse_array_command(start_buff, buff_len, &command);
                    if(consumed == 0){
                        break;
                    } else if(consumed < 0){
                        printf("Parsing error. server shutting down.");
                        exit(1);
                    }
                    
                    enum ParseResult res = execute_command(clientfd, command);
                    if(res == EXECUTE_ERR){
                        printf("Parsing error. server shutting down.");
                        exit(1);
                    }
                    ib->read_idx += consumed;
                    free_command(&command);
                } else {
                    // send protocol error.
                    break;
                }
            }
            
        }
        ib->read_idx = 0;
        ib->used = 0;
        close(clientfd);
    }
}
