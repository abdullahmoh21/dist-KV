#include <store/buffer.h>

enum ExpansionStatus expand_buffer(struct InputBuffer *ib){
    if(ib->capacity*2 > MAX_EXCEEDED && ib->capacity != MAX_EXCEEDED){  //set to exactly MAX_EXCEEDED

    } else if (ib->capacity*2 < MAX_EXCEEDED) {

    } else {
        return MAX_EXCEEDED;
    }
}