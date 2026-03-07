#ifndef BUFFER_H  
#define BUFFER_H

#include <stddef.h>
#define MAX_EXCEEDED (16 * 1024 * 1024)

enum ExpansionResult {
    EXPANSION_OK,
    EXPANSION_OOM,
    EXPANSION_MAX_EXCEEDED,
};

struct Buffer {
    char *data;
    size_t capacity;
    size_t used;        // end of data. where recv writes
    size_t read_idx;    // start of data. where parser starts
};

int expand_buffer(struct Buffer *buff);

#endif