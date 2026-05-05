#ifndef REPLICATION_H
#define REPLICATION_H

#include <stddef.h>    
#include <sys/types.h> 
#include <store/buffer.h>

typedef struct Replica{
    int fd;
    struct Buffer *buff;
    size_t offset;
}Replica;

#endif