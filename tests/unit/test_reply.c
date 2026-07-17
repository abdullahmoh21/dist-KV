#include "test_framework.h"
#include "engine/execution_engine.h"
#include "engine/reply.h"
#include "store/object.h"
#include <sys/socket.h>
#include <unistd.h>
#include <stdint.h>

/*
 * Direct reply.c coverage. Two paths:
 *   1. clientfd == -1  -> every sender short-circuits to EE_OK (the AOF-replay
 *      path, where there is no client to answer).
 *   2. no reply writer installed -> _sendRaw falls through to the real send()
 *      syscall loop. We drive it over a socketpair and read the bytes back.
 * Also exercises sendBulkArray, which no command handler happens to call.
 */

static char g_rbuf[8192];

static ssize_t drain(int fd) {
    return recv(fd, g_rbuf, sizeof(g_rbuf) - 1, 0);
}

int run_reply_tests(void) {
    TF_SUITE_BEGIN();

    /* --- clientfd == -1: senders are no-ops returning EE_OK --- */
    TEST("fd == -1 short-circuits") {
        ee_set_reply_writer(NULL, NULL);
        CHECK_EQ_INT(sendOK(-1), EE_OK);
        CHECK_EQ_INT(sendError(-1, "x"), EE_OK);
        CHECK_EQ_INT(sendNotFound(-1), EE_OK);
        CHECK_EQ_INT(sendInt(-1, 5), EE_OK);
        CHECK_EQ_INT(sendInt64(-1, 5), EE_OK);
        CHECK_EQ_INT(sendArrayHeader(-1, 2), EE_OK);
        CHECK_EQ_INT(sendBulkString(-1, "x", 1), EE_OK);
        CHECK_EQ_INT(sendSimpleString(-1, "x", 1), EE_OK);
        CHECK_EQ_INT(sendBulkArray(-1, NULL, 0), EE_OK);
    }

    /* --- real send() path over a socketpair (no writer installed) --- */
    int sv[2];
    CHECK_EQ_INT(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    ee_set_reply_writer(NULL, NULL);   /* force _sendRaw -> send() */

    TEST("sendOK writes +OK") {
        sendOK(sv[0]);
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, "+OK\r\n");
    }
    TEST("sendSimpleString") {
        sendSimpleString(sv[0], "PONG", 4);
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, "+PONG\r\n");
    }
    TEST("sendError formats -ERR") {
        sendError(sv[0], "boom");
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, "-ERR boom\r\n");
    }
    TEST("sendError over-long message rejected") {
        char big[512];
        memset(big, 'a', sizeof(big) - 1); big[sizeof(big) - 1] = '\0';
        CHECK_EQ_INT(sendError(sv[0], big), EE_ERR);   /* snprintf truncation guard */
    }
    TEST("sendNotFound") {
        sendNotFound(sv[0]);
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, "$-1\r\n");
    }
    TEST("sendInt / sendInt64 incl. negatives") {
        sendInt(sv[0], -7);
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, ":-7\r\n");
        sendInt64(sv[0], -9223372036854775807LL);
        n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, ":-9223372036854775807\r\n");
    }
    TEST("sendArrayHeader") {
        sendArrayHeader(sv[0], 3);
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, "*3\r\n");
    }
    TEST("sendBulkString small (inline)") {
        sendBulkString(sv[0], "hello", 5);
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, "$5\r\nhello\r\n");
    }
    TEST("sendBulkString large (3-write path)") {
        static char big[5000];
        memset(big, 'z', sizeof(big));
        sendBulkString(sv[0], big, sizeof(big));  // > BULK_INLINE_MAX
        /* just drain; header + body + CRLF arrive across reads */
        size_t total = 0;
        while (total < sizeof(big) + 8) {
            ssize_t n = drain(sv[1]);
            if (n <= 0) break;
            total += (size_t)n;
        }
        CHECK(total >= sizeof(big));
    }
    TEST("sendBulkArray of RedisObjects") {
        RedisObject a = {0}, b = {0};
        a.data = "aa"; a.data_len = 2;
        b.data = "bbb"; b.data_len = 3;
        const RedisObject *items[2] = {&a, &b};
        CHECK_EQ_INT(sendBulkArray(sv[0], items, 2), EE_OK);
        ssize_t n = drain(sv[1]); g_rbuf[n > 0 ? n : 0] = '\0';
        CHECK_STR_EQ(g_rbuf, "*2\r\n$2\r\naa\r\n$3\r\nbbb\r\n");
    }

    close(sv[0]); close(sv[1]);
    ee_set_reply_writer(NULL, NULL);
    TF_SUITE_END();
}
