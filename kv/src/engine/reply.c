#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include "engine/execution_engine.h"
#include "engine/reply.h"
#include "utils/fast_format.h"

// Values up to this many bytes are formatted into a single stack buffer so
// sendBulkString becomes one write call instead of three.
#define BULK_INLINE_MAX 4096

static ReplyWriteFn g_reply_writer = NULL;
static void *g_reply_writer_ctx = NULL;

void ee_set_reply_writer(ReplyWriteFn writer, void *ctx) {
    g_reply_writer = writer;
    g_reply_writer_ctx = ctx;
}

static ExecuteResult _sendRaw(int clientfd, const char *buff, size_t len) {
    if (clientfd == -1) {
        return EE_OK;
    }
    if (g_reply_writer) {
        return g_reply_writer(clientfd, buff, len, g_reply_writer_ctx);
    }

    size_t total_sent = 0;
    while (total_sent < len) {
        ssize_t n = send(clientfd, buff + total_sent, len - total_sent, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return EE_ERR;
            }
            return EE_ERR;
        } else if (n == 0) {
            return EE_SOCK_CLOSED;
        }
        total_sent += (size_t)n;
    }
    return EE_OK;
}

ExecuteResult sendSimpleString(int clientfd, const char *str, size_t len) {
    if (clientfd == -1) {
        return EE_OK;
    }
    char buf[256];
    int blen = snprintf(buf, sizeof(buf), "+%.*s\r\n", (int)len, str);
    return _sendRaw(clientfd, buf, blen);
}

ExecuteResult sendOK(int clientfd) {
    if (clientfd == -1) {
        return EE_OK;
    }
    return _sendRaw(clientfd, "+OK\r\n", 5);
}

ExecuteResult sendError(int clientfd, char *message) {
    if (clientfd == -1) {
        return EE_OK;
    }
    char response[256];
    int len = snprintf(response, sizeof(response), "-ERR %s\r\n", message);
    if (len < 0 || (size_t)len >= sizeof(response)) return EE_ERR;
    return _sendRaw(clientfd, response, (size_t)len);
}

ExecuteResult sendNotFound(int clientfd) {
    if (clientfd == -1) {
        return EE_OK;
    }
    return _sendRaw(clientfd, "$-1\r\n", 5);
}

ExecuteResult sendInt(int clientfd, int integerToSend) {
    if (clientfd == -1) {
        return EE_OK;
    }
    char buf[23];
    int len = snprintf(buf, sizeof(buf), ":%d\r\n", integerToSend);
    return _sendRaw(clientfd, buf, len);
}

ExecuteResult sendInt64(int clientfd, long long integerToSend) {
    if (clientfd == -1) {
        return EE_OK;
    }
    char buf[24]; // ':' + up to 20 digits + sign + "\r\n"
    int len = snprintf(buf, sizeof(buf), ":%lld\r\n", integerToSend);
    return _sendRaw(clientfd, buf, len);
}

ExecuteResult sendArrayHeader(int clientfd, int count) {
    if (clientfd == -1) {
        return EE_OK;
    }
    char h_buff[32];
    int h_len = snprintf(h_buff, sizeof(h_buff), "*%d\r\n", count);
    if (h_len < 0 || (u_long)h_len >= sizeof(h_buff)) return EE_ERR;
    return _sendRaw(clientfd, h_buff, h_len);
}

// RESP2 null array ("*-1\r\n") — distinct from the empty array "*0\r\n".
// Used by BZPOPMIN to signal "timed out, nothing claimed".
ExecuteResult sendNullArray(int clientfd) {
    if (clientfd == -1) {
        return EE_OK;
    }
    return _sendRaw(clientfd, "*-1\r\n", 5);
}

ExecuteResult sendBulkString(int clientfd, const char *data, size_t data_len) {
    if (clientfd == -1) {
        return EE_OK;
    }
    // Small values: format "$N\r\n<data>\r\n" into a stack buffer — one write.
    // Large values: three writes — they land contiguous in the output buffer.
    if (data_len <= BULK_INLINE_MAX) {
        char buf[BULK_INLINE_MAX + 25]; // 25 = '$' + 20 digits + "\r\n" + "\r\n"
        int hdr_len = fmt_bulk_hdr(buf, data_len);
        memcpy(buf + hdr_len, data, data_len);
        buf[hdr_len + data_len]     = '\r';
        buf[hdr_len + data_len + 1] = '\n';
        return _sendRaw(clientfd, buf, (size_t)hdr_len + data_len + 2);
    }

    char hdr[23];
    int hdr_len = fmt_bulk_hdr(hdr, data_len);
    ExecuteResult r = _sendRaw(clientfd, hdr, (size_t)hdr_len);
    if (r != EE_OK) return r;
    r = _sendRaw(clientfd, data, data_len);
    if (r != EE_OK) return r;
    return _sendRaw(clientfd, "\r\n", 2);
}

ExecuteResult sendBulkArray(int clientfd, const RedisObject **items, int count) {
    if (clientfd == -1) {
        return EE_OK;
    }
    ExecuteResult res = sendArrayHeader(clientfd, count);
    if (res != EE_OK) return res;
    for (int i = 0; i < count; i++) {
        ExecuteResult str_sent = sendBulkString(clientfd, items[i]->data, items[i]->data_len);
        if (str_sent != EE_OK) return str_sent;
    }
    return EE_OK;
}
