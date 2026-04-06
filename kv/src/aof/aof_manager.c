#include "aof_internal.h"

// --- atfork handlers: quiesce the AOF thread's mutex before fork() ---
// This prevents SIGBUS in the child caused by malloc's internal state
// being half-updated if the AOF thread was inside malloc/free at fork time.
// The handlers run with POSIX ordering guarantees relative to libc's own
// malloc atfork handlers, so the heap is clean when the child resumes.
static AOFManager *g_aof_mgr = NULL;

static void _atfork_prepare(void) {
    if (g_aof_mgr) pthread_mutex_lock(&g_aof_mgr->lock);
}
static void _atfork_parent(void) {
    if (g_aof_mgr) pthread_mutex_unlock(&g_aof_mgr->lock);
}
static void _atfork_child(void) {
    if (g_aof_mgr) pthread_mutex_unlock(&g_aof_mgr->lock);
}

static void _force_swap_buffers(AOFManager *aof);
static int _try_swap_buffers(AOFManager *aof);
static void __swap_buffers(AOFManager *aof);
static int cmd_space_avail(size_t cmd_len, struct Buffer *buff);
static void _buffer_append(struct Buffer *buf, RedisCommand *cmd);
static void* aof_thread(void *arg);

enum AOF_RESULT aof_create(AOFManager **out){
    int aof_fd = open("appendonly.aof", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd == -1) {
        // perror("open");
    }

    AOFManager *mgr = malloc(sizeof(AOFManager));
    if(mgr == NULL) return AOF_OOM;

    struct Buffer *active = malloc(sizeof(struct Buffer));
    if(active == NULL){
        free(mgr);
        return AOF_OOM;
    }

    active->data = calloc(1, INITIAL_BUFF_CAPACITY);
    active->capacity = INITIAL_BUFF_CAPACITY;
    active->max_capacity = MAX_AOF_BUF_LIMIT;
    active->read_idx = 0;
    active->used = 0;

    if(active->data == NULL){
        free(mgr);
        free(active);
        close(aof_fd);
        return AOF_OOM;
    }


    struct Buffer *standby = malloc(sizeof(struct Buffer));
    if(standby == NULL){
        free(mgr);
        free(active->data);
        free(active);
        close(aof_fd);
        return AOF_OOM;
    }

    standby->data = calloc(1, INITIAL_BUFF_CAPACITY);
    standby->capacity = INITIAL_BUFF_CAPACITY;
    standby->max_capacity = MAX_AOF_BUF_LIMIT;
    standby->read_idx = 0;
    standby->used = 0;

    if(standby->data == NULL){
        free(mgr);
        free(standby);
        free(active->data);
        free(active);
        close(aof_fd);
        return AOF_OOM;
    }

    mgr->active = active;
    mgr->standby = standby;
    mgr->shutdown = 0;
    mgr->ready_to_flush = 0;
    mgr->write_in_progress = 0;
    mgr->last_flush_ms = monotonic_ms();
    struct stat st;
    mgr->file_size = (fstat(aof_fd, &st) == 0) ? (uint64_t)st.st_size : 0;
    mgr->last_compaction_file_size = (fstat(aof_fd, &st) == 0) ? (uint64_t)st.st_size : 0;
    mgr->child_pid = -1;
    mgr->fd = aof_fd;
    pthread_mutex_init(&mgr->lock, NULL);
    pthread_cond_init(&mgr->cond, NULL);
    if (pthread_create(&mgr->thread_id, NULL, aof_thread, mgr) != 0) {
        // perror("pthread_create");
        free(mgr);
        free(standby->data);
        free(standby);
        free(active->data);
        free(active);
        close(aof_fd);
        return AOF_ERR; 
    }
    *out = mgr;
    g_aof_mgr = mgr;
    pthread_atfork(_atfork_prepare, _atfork_parent, _atfork_child);
    return AOF_OK;
}

enum AOF_RESULT aof_add(AOFManager *aof, RedisCommand *command) {
    size_t cmd_len = command->raw_len;

    if (cmd_len > aof->active->max_capacity) {
        return AOF_OOM;
    }

    // 2. Attempt: Direct fit
    if (cmd_space_avail(cmd_len, aof->active)) {
        _buffer_append(aof->active, command);
        return AOF_OK;
    }

    // 3. Attempt: Non-blocking swap to standby
    if (_try_swap_buffers(aof)) {
        if (cmd_space_avail(cmd_len, aof->active)) {
            _buffer_append(aof->active, command);
            return AOF_OK;
        }
    }

    // 4. Attempt: Expansion (Up to max_capacity)
    if (aof->active->used + cmd_len <= aof->active->max_capacity) {
        if (expand_buffer_to(aof->active, aof->active->used + cmd_len)) {
            _buffer_append(aof->active, command);
            return AOF_OK;
        }
    }

    // 5. Last Resort: Backpressure Stall
    _force_swap_buffers(aof);

    // 6. Attempt: Might still need to expand new active
    if (!cmd_space_avail(cmd_len, aof->active)) {
        if (!expand_buffer_to(aof->active, cmd_len)) {
            return AOF_OOM; // Should be impossible
        }
    }

    _buffer_append(aof->active, command);
    return AOF_OK;
}

void* aof_thread(void *arg) {
    AOFManager *aof = (AOFManager *)arg;
    while(!aof->shutdown){
        pthread_mutex_lock(&aof->lock);

        while(!aof->ready_to_flush){
            pthread_cond_wait(&aof->cond, &aof->lock);
        }
        aof->write_in_progress = 1;
        pthread_mutex_unlock(&aof->lock);


        while(aof->standby->read_idx < aof->standby->used){
            ssize_t n = write(aof->fd,
                aof->standby->data + aof->standby->read_idx,
                aof->standby->used - aof->standby->read_idx);

            if(n == -1){
                if (errno == EINTR) continue;
                perror("AOF Write Error"); 
                if (errno == ENOSPC) {
                    // critical disk failure
                    exit(1);
                }
                break;
            }
            aof->standby->read_idx += n;
            atomic_fetch_add_explicit(&aof->file_size, (uint64_t)n, memory_order_relaxed);
        }
        fdatasync(aof->fd);
        pthread_mutex_lock(&aof->lock);
        aof->standby->read_idx = 0;
        aof->standby->used = 0;
        aof->write_in_progress = 0;
        aof->ready_to_flush = 0;
        pthread_cond_broadcast(&aof->cond);
        pthread_mutex_unlock(&aof->lock);
    }
    return NULL;
}

enum AOF_RESULT aof_check_flush(AOFManager *aof){
    if(aof == NULL) return AOF_ERR;
    if(aof->active->used == 0) return AOF_OK;
    if(monotonic_ms() - aof->last_flush_ms < 1000) return AOF_OK;

    _force_swap_buffers(aof);

    return AOF_OK;
}

enum AOF_RESULT aof_force_flush(AOFManager *aof){
    if(aof == NULL) return AOF_ERR;
    if(aof->active->used == 0) return AOF_OK;

    _force_swap_buffers(aof);

    return AOF_OK;
}

void aof_destroy(AOFManager *aof) {
    if (!aof) return;

    pthread_mutex_lock(&aof->lock);
    aof->shutdown = 1; // You'll need to add this int to your struct
    pthread_cond_broadcast(&aof->cond);
    pthread_mutex_unlock(&aof->lock);

    // 2. Wait for the background thread to finish its last write
    pthread_join(aof->thread_id, NULL);

    // 3. Free everything in reverse order of creation
    if (aof->active) {
        free(aof->active->data);
        free(aof->active);
    }
    if (aof->standby) {
        free(aof->standby->data);
        free(aof->standby);
    }
    
    close(aof->fd);
    pthread_mutex_destroy(&aof->lock);
    pthread_cond_destroy(&aof->cond);
    free(aof);
}

// Helpers
static int _try_swap_buffers(AOFManager *aof) {
    if (pthread_mutex_trylock(&aof->lock) != 0) return 0;

    int success = 0;
    // Only swap if the system is actually idle
    if (!aof->ready_to_flush && !aof->write_in_progress) {
        __swap_buffers(aof);
        success = 1;
    }

    pthread_mutex_unlock(&aof->lock);
    return success;
}

static void _force_swap_buffers(AOFManager *aof) {
    pthread_mutex_lock(&aof->lock);
    
    while(aof->ready_to_flush || aof->write_in_progress) {
        pthread_cond_wait(&aof->cond, &aof->lock);
    }
    
    __swap_buffers(aof);
    
    pthread_mutex_unlock(&aof->lock);
}

// Must hold lock, NO WAITING
static void __swap_buffers(AOFManager *aof) {
    struct Buffer *tmp = aof->active;
    aof->active = aof->standby;
    aof->standby = tmp;

    aof->ready_to_flush = 1;
    aof->last_flush_ms = monotonic_ms();
    pthread_cond_signal(&aof->cond);
}

static int cmd_space_avail(size_t cmd_len, struct Buffer *buff){
    size_t buf_available = buff->capacity - buff->used;
    if(cmd_len > buf_available){
        return 0;
    }
    return 1;
}

static void _buffer_append(struct Buffer *buf, RedisCommand *cmd) {
    memcpy(buf->data + buf->used, cmd->raw_start, cmd->raw_len);
    buf->used += cmd->raw_len;
}