#include <store/buffer.h>
#include <stdlib.h>

int expand_buffer_to(struct Buffer *buff, size_t min_needed){
    if (buff->capacity >= buff->max_capacity) {
        return 0;
    }

    size_t new_size = buff->capacity * 2;

    if (new_size < min_needed) {
        new_size = min_needed;
    }

    if (new_size > buff->max_capacity) {
        new_size = buff->max_capacity;
    }

    if (new_size < min_needed) {
        return 0;
    }

    void *temp = realloc(buff->data, new_size);
    if (temp == NULL) {
        return 0;
    }

    buff->data = temp;
    buff->capacity = new_size;
    return 1;
}

// Simple wrapper for standard "next-step" expansion
int expand_buffer(struct Buffer *buff) {
    return expand_buffer_to(buff, buff->capacity * 2);
}