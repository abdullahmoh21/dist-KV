#include <store/buffer.h>
#include <stdlib.h>

int expand_buffer(struct Buffer *buff){
    if(buff->capacity == MAX_EXCEEDED){
        return 0;
    }
    
    size_t new_size;
    if(buff->capacity*2 > MAX_EXCEEDED){
        new_size = MAX_EXCEEDED;
    } else {
        new_size = buff->capacity*2;
    }
    void *temp = realloc(buff->data, new_size);
    if(temp == NULL){
        return 0;
    } 
    buff->data = temp;
    buff->capacity = new_size;
    return 1;
}