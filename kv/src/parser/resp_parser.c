#include <store/buffer.h>
#include <parser/resp_parser.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// HELPER PROTOTYPES START
static int advance(char *buff, size_t buff_len, size_t *cursor, char search);
static int advance_till_delimeter(char *buff, size_t buff_len, size_t *cursor);
static int parse_int(char *buff, size_t start, size_t len, ssize_t *out);
static int consume_delimiter(char *buff, size_t *cursor, size_t buff_len);
static void parse_bulk_string(char *buff, size_t *cursor, size_t bytes_to_read, struct BulkString *out);
void free_command(struct RedisCommand *cmd);
// HELPER PROTOTYPES END

ssize_t parse_array_command(char *buff, size_t buff_len, struct RedisCommand *out) {
    if (buff_len == 0) {
        return PARSE_INCOMPLETE;
    }
    if (buff[0] != '*') {
        return ERR_INVALID_TYPE;
    } 

    size_t tmp_cursor = 0;
    if (!advance(buff, buff_len, &tmp_cursor, '*')) return PARSE_INCOMPLETE;

    size_t start = tmp_cursor;
    if (!advance_till_delimeter(buff, buff_len, &tmp_cursor)) return PARSE_INCOMPLETE;

    ssize_t array_len;
    if (!parse_int(buff, start, tmp_cursor - start, &array_len)) {
        return ERR_INVALID_ARRAY_L;
    }

    if (array_len < 0) return ERR_INVALID_ARRAY_L;
    if (array_len > MAX_ARGS) return ERR_ARRAY_TOO_BIG;

    if (!consume_delimiter(buff, &tmp_cursor, buff_len)) {
        return PARSE_INCOMPLETE;
    }

    // Allocate into a local pointer first
    struct BulkString *strings = calloc(array_len, sizeof(struct BulkString));
    if (strings == NULL) return ERR_MEM_ALLOC;

    for (int i = 0; i < array_len; i++) {
        if (!advance(buff, buff_len, &tmp_cursor, '$')) {
            free(strings);
            return (tmp_cursor < buff_len) ? ERR_INVALID_BULK_P : PARSE_INCOMPLETE;
        }

        size_t b_start = tmp_cursor;
        if (!advance_till_delimeter(buff, buff_len, &tmp_cursor)) {
            free(strings);
            return PARSE_INCOMPLETE;
        }

        ssize_t bytes_to_read;
        if (!parse_int(buff, b_start, tmp_cursor - b_start, &bytes_to_read)) {
            free(strings);
            return ERR_INVALID_BULK_L;
        }

        if (bytes_to_read > MAX_BULK_LEN) {
            free(strings);
            return ERR_BULK_TOO_BIG;
        }

        // Handle the \r\n after the length ($5\r\n...)
        if (!consume_delimiter(buff, &tmp_cursor, buff_len)) {
            free(strings);
            return PARSE_INCOMPLETE;
        }

        if (bytes_to_read == -1) { 
            strings[i].data = NULL; 
            strings[i].len = 0;
            // No data to read, and we already consumed the delimiter after the '-1'
            continue;
        }

        // Check if data + trailing \r\n exists
        if (buff_len - tmp_cursor < (size_t)bytes_to_read + 2) {
            free(strings);
            return PARSE_INCOMPLETE;
        }
       
        parse_bulk_string(buff, &tmp_cursor, bytes_to_read, &strings[i]);

        // Consume the final \r\n after the bulk data
        if (!consume_delimiter(buff, &tmp_cursor, buff_len)) {
            free(strings);
            return PARSE_INCOMPLETE;
        }
    }
    
    out->args = strings;
    out->arg_count = array_len;
    return (ssize_t)tmp_cursor; 
}


static int advance(char *buff, size_t buff_len, size_t *cursor, char search){
    char *pos = memchr(buff + *cursor, search, buff_len - *cursor);
    if(pos == NULL){ return 0; }
    *cursor = (size_t)(pos - buff) +1;
    return 1;
}

static int advance_till_delimeter(char *buff, size_t buff_len, size_t *cursor){
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

static int parse_int(char *buff, size_t start, size_t len, ssize_t *out){
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

static int consume_delimiter(char *buff, size_t *cursor, size_t buff_len){
    if(*cursor + 1 < buff_len && buff[*cursor] == '\r' && buff[*cursor + 1] == '\n'){
        *cursor += 2;   // Move past BOTH \r and \n
        return 1;
    }
    return 0;
}

static void parse_bulk_string(char *buff, size_t *cursor, size_t bytes_to_read, struct BulkString *out){
    out->data = buff + *cursor;
    out->len = bytes_to_read;
    *cursor += bytes_to_read;   // move cursor to end
    return;
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