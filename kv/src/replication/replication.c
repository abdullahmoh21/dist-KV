#include "replication/replication.h"
#include "parser/resp_parser.h"
#include "engine/execution_engine.h"
#include "event_loop/event_loop.h"
#include "utils/time.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/wait.h>

#define SNAPSHOT_CHUNK          (64 * 1024)
#define REPL_DRAIN_CHUNK        (64 * 1024)
#define REPL_CLIENT_RECV_CAP    (4ULL * 1024 * 1024)
#define REPL_CLIENT_RECV_MAX    (64ULL * 1024 * 1024)
#define REPL_CLIENT_SEND_CAP    (64 * 1024)
#define REPL_CLIENT_SEND_MAX    (1024 * 1024)
#define ACK_INTERVAL_MS         1000

/* ─── Global context pointers (set by server at init) ─────────────── */

static Replica ***g_replicas      = NULL;
static int       *g_replica_count = NULL;
static uint64_t  *g_repl_offset   = NULL;

void repl_set_context(Replica ***replicas, int *replica_count, uint64_t *repl_offset) {
    g_replicas      = replicas;
    g_replica_count = replica_count;
    g_repl_offset   = repl_offset;
}

uint64_t repl_get_offset(void) {
    return g_repl_offset ? *g_repl_offset : 0;
}

int repl_count_synced(uint64_t target_offset) {
    if (!g_replicas || !g_replica_count || !*g_replicas) return 0;
    int count = 0;
    for (int i = 0; i < *g_replica_count; i++) {
        Replica *r = (*g_replicas)[i];
        if (r && r->offset >= target_offset) count++;
    }
    return count;
}

/* ─── Shared replication backlog ─────────────────────────────────── */

static ReplBacklog g_backlog;

void repl_backlog_init(size_t cap) {
    g_backlog.data        = malloc(cap);
    g_backlog.cap         = cap;
    g_backlog.read_head   = 0;
    g_backlog.write_head  = 0;
    g_backlog.used        = 0;
    g_backlog.base_offset = 0;
}

/* Advance base_offset using only STREAMING replicas as the watermark.
   FULL_SYNC / SENDING_SNAPSHOT replicas have a frozen acked cursor — they
   are validated separately when they transition to STREAMING. */
static void _backlog_compact(void) {
    if (!g_replicas || !g_replica_count || *g_replica_count == 0) {
        /* No replicas at all — drain the ring so it stays available for
           future connections (partial resync would reuse it). */
        g_backlog.base_offset += g_backlog.used;
        g_backlog.read_head    = g_backlog.write_head;
        g_backlog.used         = 0;
        return;
    }

    uint64_t min_acked = UINT64_MAX;
    int      streaming = 0;
    for (int i = 0; i < *g_replica_count; i++) {
        Replica *r = (*g_replicas)[i];
        if (!r || r->dead || r->state != REPLICA_STREAMING) continue;
        if (r->acked < min_acked) min_acked = r->acked;
        streaming++;
    }

    if (streaming == 0 || min_acked == UINT64_MAX) return;
    if (min_acked <= g_backlog.base_offset) return;

    size_t drop = (size_t)(min_acked - g_backlog.base_offset);
    if (drop > g_backlog.used) drop = g_backlog.used;

    g_backlog.read_head    = (g_backlog.read_head + drop) % g_backlog.cap;
    g_backlog.used        -= drop;
    g_backlog.base_offset  = min_acked;
}

/* Evict STREAMING replicas whose lag would overflow the ring after writing len
   bytes.  FULL_SYNC / SENDING_SNAPSHOT replicas are checked on transition. */
static void _backlog_evict_slow(size_t len) {
    if (!g_replicas || !g_replica_count || !g_repl_offset) return;
    uint64_t after = *g_repl_offset + (uint64_t)len;
    for (int i = 0; i < *g_replica_count; i++) {
        Replica *r = (*g_replicas)[i];
        if (!r || r->dead || r->state != REPLICA_STREAMING) continue;
        if (after - r->acked > g_backlog.cap) {
            fprintf(stderr,
                "[repl] replica fd=%d evicted — lag %"PRIu64" B exceeds backlog cap %zu B\n",
                r->fd, after - r->acked, g_backlog.cap);
            r->dead = 1;
        }
    }
}

/* Low-level ring write — caller guarantees there is room. */
static void _ring_write(const char *data, size_t len) {
    size_t wh    = g_backlog.write_head;
    size_t cap   = g_backlog.cap;
    size_t first = cap - wh;
    if (first >= len) {
        memcpy(g_backlog.data + wh, data, len);
    } else {
        memcpy(g_backlog.data + wh, data, first);
        memcpy(g_backlog.data, data + first, len - first);
    }
    g_backlog.write_head = (wh + len) % cap;
    g_backlog.used      += len;
}

/* Append len bytes to the ring.  Evicts slow replicas and compacts first. */
static void _backlog_append(const char *data, size_t len) {
    if (!g_backlog.data || len == 0) return;

    if (len >= g_backlog.cap) {
        /* Single command larger than the whole ring — evict everything and
           write only the last cap bytes (this should never happen in practice
           since a command can't exceed the client output buffer). */
        if (g_replicas && g_replica_count) {
            for (int i = 0; i < *g_replica_count; i++) {
                Replica *r = (*g_replicas)[i];
                if (r && !r->dead && r->state == REPLICA_STREAMING) r->dead = 1;
            }
        }
        g_backlog.read_head   = 0;
        g_backlog.write_head  = 0;
        g_backlog.used        = 0;
        _ring_write(data + (len - g_backlog.cap), g_backlog.cap);
        return;
    }

    _backlog_evict_slow(len);
    _backlog_compact();

    /* Compact may not have freed enough if there are only non-STREAMING
       replicas (FULL_SYNC etc.).  Forcibly advance base_offset to make room —
       those replicas will be caught and evicted when they transition. */
    if (g_backlog.used + len > g_backlog.cap) {
        size_t overflow = (g_backlog.used + len) - g_backlog.cap;
        g_backlog.read_head    = (g_backlog.read_head + overflow) % g_backlog.cap;
        g_backlog.used        -= overflow;
        g_backlog.base_offset += overflow;
    }

    _ring_write(data, len);
}

int repl_backlog_has_data(const Replica *r) {
    return g_repl_offset && (r->acked < *g_repl_offset);
}

/* Drain up to REPL_DRAIN_CHUNK bytes of the backlog for replica r.
   Returns bytes sent (≥0) or -1 if the connection should be closed. */
int repl_backlog_drain_replica(Replica *r) {
    if (!g_backlog.data || !g_repl_offset) return 0;

    if (r->acked < g_backlog.base_offset) {
        /* Ring wrapped past this replica's read position — it missed data and
           must reconnect for a full resync. */
        fprintf(stderr,
            "[repl] replica fd=%d evicted — acked offset %"PRIu64
            " is behind backlog base %"PRIu64"\n",
            r->fd, r->acked, g_backlog.base_offset);
        r->dead = 1;
        return -1;
    }

    uint64_t lag = *g_repl_offset - r->acked;
    if (lag == 0) return 0;

    size_t to_send = (size_t)(lag < REPL_DRAIN_CHUNK ? lag : REPL_DRAIN_CHUNK);
    size_t skip    = (size_t)(r->acked - g_backlog.base_offset);
    size_t rh      = (g_backlog.read_head + skip) % g_backlog.cap;

    ssize_t n;
    size_t first = g_backlog.cap - rh;
    if (first >= to_send) {
        n = send(r->fd, g_backlog.data + rh, to_send, MSG_NOSIGNAL);
    } else {
        /* Data wraps around the ring end — use scatter I/O. */
        struct iovec iov[2];
        iov[0].iov_base = g_backlog.data + rh;
        iov[0].iov_len  = first;
        iov[1].iov_base = g_backlog.data;
        iov[1].iov_len  = to_send - first;
        n = writev(r->fd, iov, 2);
    }

    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return 0;
        return -1;
    }
    if (n == 0) return -1;

    r->acked += (uint64_t)n;
    return (int)n;
}

/* ─── Buffer helpers (used for snapshot and replica-client I/O) ──── */

static struct Buffer *_alloc_buf(size_t cap, size_t max_cap) {
    struct Buffer *b = malloc(sizeof(struct Buffer));
    if (!b) return NULL;
    b->data = malloc(cap);
    if (!b->data) { free(b); return NULL; }
    b->capacity     = cap;
    b->max_capacity = max_cap;
    b->used         = 0;
    b->read_idx     = 0;
    return b;
}

static void _free_buf(struct Buffer *b) {
    if (!b) return;
    free(b->data);
    free(b);
}

static int _buf_append(struct Buffer *b, const char *data, size_t len) {
    if (!b || !data || len == 0) return 1;
    if (b->read_idx == b->used) { b->read_idx = 0; b->used = 0; }
    if (b->read_idx > 0 && (b->capacity - b->used) < len) {
        size_t pending = b->used - b->read_idx;
        memmove(b->data, b->data + b->read_idx, pending);
        b->read_idx = 0;
        b->used = pending;
    }
    if (b->used + len > b->capacity) {
        if (!expand_buffer_to(b, b->used + len)) return 0;
    }
    memcpy(b->data + b->used, data, len);
    b->used += len;
    return 1;
}

/* ─── Replica lifecycle ───────────────────────────────────────────── */

int repl_add_replica(int fd, struct Buffer *send_buf, uint64_t start_acked) {
    if (!g_replicas || !g_replica_count) return -1;

    Replica *r = calloc(1, sizeof(Replica));
    if (!r) return -1;

    r->fd               = fd;
    r->send_buf         = send_buf;   // shared with ClientState.out_buffer, snapshot only
    r->acked            = start_acked;
    r->state            = REPLICA_HANDSHAKE;
    r->sync_child_pid   = -1;
    r->sync_send_fd     = -1;
    r->dead             = 0;

    int idx = *g_replica_count;
    Replica **arr = realloc(*g_replicas, sizeof(Replica*) * (size_t)(idx + 1));
    if (!arr) { free(r); return -1; }
    arr[idx] = r;
    *g_replicas = arr;
    (*g_replica_count)++;
    return 0;
}

void repl_remove_replica(int fd) {
    if (!g_replicas || !g_replica_count || !*g_replicas) return;
    int count = *g_replica_count;
    for (int i = 0; i < count; i++) {
        Replica *r = (*g_replicas)[i];
        if (!r || r->fd != fd) continue;
        /* send_buf is owned by ClientState — not freed here */
        if (r->sync_send_fd >= 0) { close(r->sync_send_fd); r->sync_send_fd = -1; }
        free(r);
        (*g_replicas)[i] = (*g_replicas)[count - 1];
        (*g_replicas)[count - 1] = NULL;
        (*g_replica_count)--;
        return;
    }
}

Replica *repl_find_replica(int fd) {
    if (!g_replicas || !g_replica_count || !*g_replicas) return NULL;
    for (int i = 0; i < *g_replica_count; i++) {
        Replica *r = (*g_replicas)[i];
        if (r && r->fd == fd) return r;
    }
    return NULL;
}

/* ─── Write propagation ───────────────────────────────────────────── */

/* O(1) regardless of replica count: one ring append.
   STREAMING replicas drain from the ring on their writable events.
   FULL_SYNC / SENDING_SNAPSHOT replicas have r->acked frozen at the PSYNC
   offset — the ring accumulates the delta they need automatically. */
void repl_propagate(RedisCommand *cmd) {
    if (g_repl_offset) *g_repl_offset += cmd->raw_len;
    _backlog_append(cmd->raw_start, cmd->raw_len);
}

/* ─── Snapshot child polling ──────────────────────────────────────── */

int repl_check_sync_children(event_loop_t *loop) {
    if (!g_replicas || !g_replica_count || *g_replica_count == 0) return 0;
    int completed = 0;

    for (int i = 0; i < *g_replica_count; i++) {
        Replica *r = (*g_replicas)[i];
        if (!r || r->state != REPLICA_FULL_SYNC || r->sync_child_pid <= 0) continue;

        int status;
        pid_t pid = waitpid(r->sync_child_pid, &status, WNOHANG);
        if (pid == 0) continue;

        r->sync_child_pid = -1;
        completed++;

        if (pid < 0 || !(WIFEXITED(status) && WEXITSTATUS(status) == 0)) {
            fprintf(stderr, "[repl] snapshot fork failed for replica fd=%d\n", r->fd);
            unlink(r->sync_file);
            r->dead = 1;
            continue;
        }

        /* Check that the ring hasn't wrapped past this replica's start point
           while the fork was running.  If it has, the delta is gone and the
           replica must reconnect for a fresh full resync. */
        if (r->acked < g_backlog.base_offset) {
            fprintf(stderr,
                "[repl] replica fd=%d: ring wrapped during fork (acked=%"PRIu64
                " base=%"PRIu64") — evicting\n",
                r->fd, r->acked, g_backlog.base_offset);
            unlink(r->sync_file);
            r->dead = 1;
            continue;
        }

        struct stat st;
        if (stat(r->sync_file, &st) != 0) {
            fprintf(stderr, "[repl] stat %s failed: %s\n", r->sync_file, strerror(errno));
            r->dead = 1;
            continue;
        }

        int sfd = open(r->sync_file, O_RDONLY);
        if (sfd < 0) {
            fprintf(stderr, "[repl] open %s failed: %s\n", r->sync_file, strerror(errno));
            r->dead = 1;
            continue;
        }

        char hdr[40];
        int hdr_len = snprintf(hdr, sizeof(hdr), "$%lld\r\n", (long long)st.st_size);
        _buf_append(r->send_buf, hdr, (size_t)hdr_len);

        r->sync_send_fd             = sfd;
        r->snapshot_bytes_remaining = (int64_t)st.st_size;
        r->state                    = REPLICA_SENDING_SNAPSHOT;

        event_loop_mod(loop, r->fd, EVENT_READABLE | EVENT_WRITABLE);
        fprintf(stderr, "[repl] replica fd=%d snapshot ready (%lld bytes)\n",
                r->fd, (long long)st.st_size);
    }
    return completed;
}

/* Stream snapshot file to replica in chunks across event loop ticks.
   After the file is fully sent the replica transitions to STREAMING and the
   backlog drain machinery takes over — no extra copy needed. */
void repl_advance_snapshot_send(event_loop_t *loop) {
    if (!g_replicas || !g_replica_count || *g_replica_count == 0) return;

    for (int i = 0; i < *g_replica_count; i++) {
        Replica *r = (*g_replicas)[i];
        if (!r || r->state != REPLICA_SENDING_SNAPSHOT) continue;

        char chunk[SNAPSHOT_CHUNK];
        ssize_t n = read(r->sync_send_fd, chunk, SNAPSHOT_CHUNK);
        if (n > 0) {
            _buf_append(r->send_buf, chunk, (size_t)n);
            r->snapshot_bytes_remaining -= n;
            event_loop_mod(loop, r->fd, EVENT_READABLE | EVENT_WRITABLE);
        }

        if (n <= 0 || r->snapshot_bytes_remaining <= 0) {
            close(r->sync_send_fd);
            r->sync_send_fd = -1;
            unlink(r->sync_file);

            /* Same ring-validity check as in repl_check_sync_children. */
            if (r->acked < g_backlog.base_offset) {
                fprintf(stderr,
                    "[repl] replica fd=%d: ring wrapped during snapshot send — evicting\n",
                    r->fd);
                r->dead = 1;
                continue;
            }

            /* r->acked is still at the PSYNC offset.  The backlog holds the
               delta since then; repl_backlog_drain_replica will pick it up
               naturally on the next writable event. */
            r->state = REPLICA_STREAMING;
            event_loop_mod(loop, r->fd, EVENT_READABLE | EVENT_WRITABLE);
            fprintf(stderr, "[repl] replica fd=%d full sync complete — streaming\n", r->fd);
        }
    }
}

/* ─── REPLCONF handler ────────────────────────────────────────────── */

int repl_handle_replconf(int clientfd, RedisCommand *cmd) {
    if (cmd->arg_count < 3) return 0;

    const char *sub = cmd->args[1].data;
    size_t sub_len  = cmd->args[1].len;

    if (sub_len == 14 && strncasecmp(sub, "listening-port", 14) == 0) {
        Replica *r = repl_find_replica(clientfd);
        if (r) r->listening_port = (uint16_t)strtol(cmd->args[2].data, NULL, 10);
        return 0;
    }

    if (sub_len == 3 && strncasecmp(sub, "ack", 3) == 0) {
        uint64_t off = (uint64_t)strtoull(cmd->args[2].data, NULL, 10);
        repl_update_ack(clientfd, off);
        return 1;  // suppress reply — replicas don't get a response to ACK
    }

    return 0;
}

void repl_update_ack(int fd, uint64_t ack_offset) {
    Replica *r = repl_find_replica(fd);
    if (r) r->offset = ack_offset;
}

/* ─── Replica-side client state machine ──────────────────────────── */

ReplClientContext *replica_client_create(int fd, uint16_t our_port) {
    ReplClientContext *ctx = calloc(1, sizeof(ReplClientContext));
    if (!ctx) return NULL;
    ctx->recv_buf = _alloc_buf(REPL_CLIENT_RECV_CAP, REPL_CLIENT_RECV_MAX);
    ctx->send_buf = _alloc_buf(REPL_CLIENT_SEND_CAP, REPL_CLIENT_SEND_MAX);
    if (!ctx->recv_buf || !ctx->send_buf) {
        _free_buf(ctx->recv_buf);
        _free_buf(ctx->send_buf);
        free(ctx);
        return NULL;
    }
    ctx->fd           = fd;
    ctx->state        = REPL_CLIENT_CONNECTING;
    ctx->our_port     = our_port;
    ctx->bulk_remaining = -1;
    return ctx;
}

void replica_client_destroy(ReplClientContext *ctx) {
    if (!ctx) return;
    _free_buf(ctx->recv_buf);
    _free_buf(ctx->send_buf);
    free(ctx);
}

static void _rc_send(ReplClientContext *ctx, const char *data, size_t len) {
    _buf_append(ctx->send_buf, data, len);
}

static const char *_find_crlf(struct Buffer *b) {
    for (size_t i = b->read_idx; i + 1 < b->used; i++) {
        if (b->data[i] == '\r' && b->data[i+1] == '\n') return b->data + i;
    }
    return NULL;
}

int replica_client_handle_readable(ReplClientContext *ctx, RedisStore *store, event_loop_t *loop) {
    struct Buffer *rb = ctx->recv_buf;
    if (rb->read_idx == rb->used) { rb->read_idx = 0; rb->used = 0; }

    if (rb->capacity - rb->used < 4096) {
        if (!expand_buffer(rb)) { fprintf(stderr, "[replica] recv OOM\n"); return -1; }
    }

    ssize_t n = recv(ctx->fd, rb->data + rb->used, rb->capacity - rb->used, 0);
    if (n <= 0) {
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return 0;
        fprintf(stderr, "[replica] lost connection to primary\n");
        return -1;
    }
    rb->used += (size_t)n;

    int keep_going = 1;
    while (keep_going) {
        keep_going = 0;
        switch (ctx->state) {

            case REPL_CLIENT_CONNECTING:
                return 0;

            case REPL_CLIENT_WAIT_PONG:
            case REPL_CLIENT_WAIT_OK_1:
            case REPL_CLIENT_WAIT_OK_2: {
                const char *crlf = _find_crlf(rb);
                if (!crlf) return 0;

                char *line = rb->data + rb->read_idx;
                size_t line_len = (size_t)(crlf - line);
                rb->read_idx += line_len + 2;

                if (line_len < 1 || line[0] != '+') {
                    fprintf(stderr, "[replica] unexpected handshake reply: %.*s\n",
                            (int)line_len, line);
                    return -1;
                }

                if (ctx->state == REPL_CLIENT_WAIT_PONG) {
                    char port_str[16];
                    int  port_len = snprintf(port_str, sizeof(port_str), "%u", ctx->our_port);
                    char msg[128];
                    int  msg_len  = snprintf(msg, sizeof(msg),
                        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
                        port_len, port_str);
                    _rc_send(ctx, msg, (size_t)msg_len);
                    ctx->state = REPL_CLIENT_WAIT_OK_1;

                } else if (ctx->state == REPL_CLIENT_WAIT_OK_1) {
                    const char capa[] = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                    _rc_send(ctx, capa, sizeof(capa) - 1);
                    ctx->state = REPL_CLIENT_WAIT_OK_2;

                } else {
                    const char psync[] = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                    _rc_send(ctx, psync, sizeof(psync) - 1);
                    ctx->state = REPL_CLIENT_WAIT_FULLRESYNC;
                }

                event_loop_mod(loop, ctx->fd, EVENT_READABLE | EVENT_WRITABLE);
                keep_going = 1;
                break;
            }

            case REPL_CLIENT_WAIT_FULLRESYNC: {
                const char *crlf = _find_crlf(rb);
                if (!crlf) return 0;

                char *line    = rb->data + rb->read_idx;
                size_t llen   = (size_t)(crlf - line);
                rb->read_idx += llen + 2;

                if (llen < 12 || strncasecmp(line, "+FULLRESYNC ", 12) != 0) {
                    fprintf(stderr, "[replica] expected +FULLRESYNC, got: %.*s\n",
                            (int)llen, line);
                    return -1;
                }
                char *sp = memchr(line + 12, ' ', llen - 12);
                if (sp) {
                    size_t id_len = (size_t)(sp - (line + 12));
                    if (id_len > 40) id_len = 40;
                    memcpy(ctx->repl_id, line + 12, id_len);
                    ctx->repl_id[id_len] = '\0';
                    ctx->repl_offset = (uint64_t)strtoull(sp + 1, NULL, 10);
                }
                ctx->state = REPL_CLIENT_RECV_BULK_HDR;
                keep_going = 1;
                break;
            }

            case REPL_CLIENT_RECV_BULK_HDR: {
                const char *crlf = _find_crlf(rb);
                if (!crlf) return 0;

                char *line    = rb->data + rb->read_idx;
                size_t llen   = (size_t)(crlf - line);
                rb->read_idx += llen + 2;

                if (llen < 1 || line[0] != '$') {
                    fprintf(stderr, "[replica] expected $<size>, got: %.*s\n",
                            (int)llen, line);
                    return -1;
                }
                ctx->bulk_remaining = (int64_t)strtoull(line + 1, NULL, 10);
                ctx->state = REPL_CLIENT_RECV_BULK;
                fprintf(stderr, "[replica] receiving snapshot (%lld bytes)\n",
                        (long long)ctx->bulk_remaining);
                keep_going = 1;
                break;
            }

            case REPL_CLIENT_RECV_BULK:
            case REPL_CLIENT_STREAMING: {
                while (rb->read_idx < rb->used) {
                    if (ctx->state == REPL_CLIENT_RECV_BULK && ctx->bulk_remaining <= 0) {
                        ctx->state = REPL_CLIENT_STREAMING;
                        fprintf(stderr, "[replica] snapshot replay done — streaming\n");
                        keep_going = 1;
                        break;
                    }

                    RedisCommand cmd;
                    ssize_t consumed = parse_array_command(
                        rb->data + rb->read_idx,
                        rb->used  - rb->read_idx,
                        &cmd);

                    if (consumed == 0) goto done;
                    if (consumed < 0)  {
                        fprintf(stderr, "[replica] parse error %zd — skipping\n", consumed);
                        rb->read_idx = rb->used;
                        goto done;
                    }

                    dispatch_command(-1, &cmd, store);
                    free_command(&cmd);

                    rb->read_idx += (size_t)consumed;
                    if (ctx->state == REPL_CLIENT_RECV_BULK) {
                        ctx->bulk_remaining -= consumed;
                    } else {
                        ctx->repl_offset += (uint64_t)consumed;
                    }
                }
                break;
            }
        }
    }
done:;
    return 0;
}

int replica_client_handle_writable(ReplClientContext *ctx, event_loop_t *loop) {
    if (ctx->state == REPL_CLIENT_CONNECTING) {
        int err = 0;
        socklen_t elen = sizeof(err);
        if (getsockopt(ctx->fd, SOL_SOCKET, SO_ERROR, &err, &elen) < 0 || err != 0) {
            fprintf(stderr, "[replica] connect to primary failed: %s\n",
                    strerror(err ? err : errno));
            return -1;
        }
        const char ping[] = "*1\r\n$4\r\nPING\r\n";
        _rc_send(ctx, ping, sizeof(ping) - 1);
        ctx->state = REPL_CLIENT_WAIT_PONG;
        event_loop_mod(loop, ctx->fd, EVENT_READABLE | EVENT_WRITABLE);
    }

    struct Buffer *sb = ctx->send_buf;
    while (sb->read_idx < sb->used) {
        ssize_t n = send(ctx->fd, sb->data + sb->read_idx,
                         sb->used - sb->read_idx, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            fprintf(stderr, "[replica] send to primary failed\n");
            return -1;
        }
        sb->read_idx += (size_t)n;
    }
    if (sb->read_idx == sb->used) { sb->read_idx = 0; sb->used = 0; }

    if (sb->used == sb->read_idx && ctx->state != REPL_CLIENT_CONNECTING) {
        event_loop_mod(loop, ctx->fd, EVENT_READABLE);
    }
    return 0;
}

void replica_client_send_ack(ReplClientContext *ctx) {
    if (ctx->state != REPL_CLIENT_STREAMING) return;
    char offset_str[24];
    int  offset_len = snprintf(offset_str, sizeof(offset_str), "%" PRIu64, ctx->repl_offset);
    char msg[128];
    int  msg_len = snprintf(msg, sizeof(msg),
        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n",
        offset_len, offset_str);
    _buf_append(ctx->send_buf, msg, (size_t)msg_len);
    ctx->last_ack_ms = monotonic_ms();
}
