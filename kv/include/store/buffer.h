#ifndef BUFFER_H  
#define BUFFER_H

#include <stddef.h>

#define CLIENT_INPUT_BUFF_LEN 4096
#define MAX_CLIENT_INPUT_BUFF_LEN (64 * 1024 * 1024)

#define INITIAL_BUFF_CAPACITY CLIENT_INPUT_BUFF_LEN
#define MAX_EXCEEDED MAX_CLIENT_INPUT_BUFF_LEN

enum ExpansionResult {
    EXPANSION_OK,
    EXPANSION_OOM,
    EXPANSION_MAX_EXCEEDED,
};

struct Buffer {
    char *data;
    size_t capacity;
    size_t max_capacity;
    size_t used;        // end of data. where recv writes
    size_t read_idx;    // start of data. 
};

int expand_buffer(struct Buffer *buff);
int expand_buffer_to(struct Buffer *buff, size_t min_needed);

#endif