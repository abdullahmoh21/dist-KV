#include <store/buffer.h>
#include <parser/resp_parser.h>
#include <stdlib.h>
#include <string.h>

// HELPER PROTOTYPES START
int advance(char *buff, size_t buff_len, size_t *cursor, char search);
int advance_till_delimeter(char *buff, size_t buff_len, size_t *cursor);
int parse_int(char *buff, size_t start, size_t len, ssize_t *out);
int consume_delimiter(char *buff, size_t *cursor, size_t buff_len);
void free_command(struct RedisCommand *cmd);
enum ParseResult parse_bulk_string(char *buff, size_t *cursor, size_t bytes_to_read, struct BulkString *out);
// HELPER PROTOTYPES END

int parse_array_command(char *buff, size_t buff_len, struct RedisCommand *out){
    size_t tmp_cursor = 0;         // cursor we can manipulate
    if(!advance(buff, buff_len, &tmp_cursor, '*')){ return 0; }
    size_t start = tmp_cursor;
    if(!advance_till_delimeter(buff, buff_len, &tmp_cursor)){ return 0; }
    ssize_t array_len;
    ssize_t len = tmp_cursor-start;
    if(!parse_int(buff, start, len, &array_len)){ return 0; }
    
    // We still need to allocate the array of BulkString headers to hold our "views"
    struct BulkString *strings = calloc(array_len, sizeof(struct BulkString));
    if(strings == NULL){ return -1;}
    out->args = strings;
    out->arg_count = array_len;

    for(int i = 0; i < array_len; i++){
        if(!advance(buff, buff_len, &tmp_cursor, '$')){
            free_command(out);
            return 0; 
        }

        size_t start = tmp_cursor;

        if(!advance_till_delimeter(buff, buff_len, &tmp_cursor)){
            free_command(out);
            return 0; 
        }

        size_t len = tmp_cursor - start;
        ssize_t bytes_to_read;
        if(!parse_int(buff, start, len, &bytes_to_read)){
            free_command(out);
            return 0; 
        }

        if(bytes_to_read == -1){    // Null Bulk String
            strings[i].data = NULL; 
            strings[i].len = 0;
            continue;
        }

        if(buff_len - tmp_cursor < bytes_to_read + 2){  // not enough bytes to read
            free_command(out);
            return 0; 
        } 
        if(!consume_delimiter(buff, &tmp_cursor, buff_len)){
            free_command(out);
            return 0;
        }
       
        enum ParseResult res = parse_bulk_string(buff, &tmp_cursor, bytes_to_read, &strings[i]);
       
        if(res == PARSE_NEED_MORE || res == PARSE_ERR){
            free_command(out);
            return 0;
        }
        if(!consume_delimiter(buff, &tmp_cursor, buff_len)){
            free_command(out);
            return 0;
        }
    }
    return tmp_cursor ;

}

// advances cursor to right after char 'search'
int advance(char *buff, size_t buff_len, size_t *cursor, char search){
    char *pos = memchr(buff + *cursor, search, buff_len - *cursor);
    if(pos == NULL){ return 0; }
    *cursor = (size_t)(pos - buff) +1;
    return 1;
}

//advances the cursor to right before the \r\n delimter
int advance_till_delimeter(char *buff, size_t buff_len, size_t *cursor){
    char *pos_r = memchr(buff + *cursor, '\r', buff_len - *cursor);
    if(pos_r == NULL || (size_t)(pos_r - buff) + 1 >= buff_len){
        return 0; 
    }

    if(*(pos_r + 1) == '\n'){
        *cursor = (size_t)(pos_r - buff); // Move to \r
        return 1;
    }
    return 0;
}

int parse_int(char *buff, size_t start, size_t len, ssize_t *out){
    ssize_t result = 0;
    int neg = 0;
    int i = 0;
    if(buff[start] == '-'){
        neg = 1;
        i = 1;
    }
    for(; i < len; i++){
        char c = buff[start + i];

        if(c < '0' || c > '9'){
            return 0;
        }

        result = (result * 10) + c - '0';
    }
    *out = neg ? -result : result;
    return 1;
}

// consumes the \r\n by moving cursor after it
int consume_delimiter(char *buff, size_t *cursor, size_t buff_len){
    if(*cursor + 1 < buff_len && buff[*cursor] == '\r' && buff[*cursor + 1] == '\n'){
        *cursor += 2;   // Move past BOTH \r and \n
        return 1;
    }
    return 0;
}

// Operates on pointers into the buffer (Zero-Copy)
enum ParseResult parse_bulk_string(char *buff, size_t *cursor, size_t bytes_to_read, struct BulkString *out){
    out->data = buff + *cursor;
    out->len = bytes_to_read;
    *cursor += bytes_to_read;   // move cursor to end
    return PARSE_OK;
}

void free_command(struct RedisCommand *cmd) {
    if (cmd == NULL) {
        return;
    }

    if (cmd->args != NULL) {
        free(cmd->args);
        cmd->args = NULL;
    }

    cmd->arg_count = 0;
}