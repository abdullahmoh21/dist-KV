#ifndef AOF_BUF_H
#define AOF_BUF_H
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include "parser/resp_parser.h"

typedef struct RedisStore RedisStore;

#define AOF_READ_BUFF_LEN 4096
#define AOF_READ_BUFF_LEN_MAX (256 * 1024 * 1024)
#define MAX_AOF_BUF_LIMIT AOF_READ_BUFF_LEN_MAX

enum AOF_RESULT {
    AOF_OK,
    AOF_ERR,
    AOF_IO_ERR,
    AOF_EXEC_ERR,
    AOF_PARSE_ERR,
    AOF_OOM
};

typedef struct {
    struct Buffer *active;
    struct Buffer *standby;
    pthread_mutex_t lock;      
    pthread_cond_t cond;       
    pthread_t thread_id;
    int fd;
    int shutdown;
    int ready_to_flush;
    int write_in_progress;
    uint64_t last_flush_ms;
} AOFManager;


enum AOF_RESULT aof_load(RedisStore *store);
enum AOF_RESULT aof_create(AOFManager **out);
enum AOF_RESULT aof_add(AOFManager *aof, RedisCommand *command);
enum AOF_RESULT aof_compact(AOFManager *aof);
enum AOF_RESULT aof_check_flush(AOFManager *aof);
void aof_destroy(AOFManager *aof);
#endif