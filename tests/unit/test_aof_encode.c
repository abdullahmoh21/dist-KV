#include "test_framework.h"
#include "store/buffer.h"
#include <stdint.h>
#include <stdlib.h>

/* aof_resp_encode.c helpers (declared in kv/src/aof/aof_internal.h, forward-
 * declared here to avoid depending on that internal path). These format the
 * lengths/integers the compaction serializer writes. */
int   _digits(size_t n);
char *__write_size_t(char *dest, size_t n);
char *itoa(uint64_t val, char *buf);
void  _append_len(struct Buffer *buf, size_t len);

int run_aof_encode_tests(void) {
    TF_SUITE_BEGIN();

    TEST("_digits across every magnitude boundary") {
        CHECK_EQ_INT(_digits(0), 1);
        CHECK_EQ_INT(_digits(9), 1);
        CHECK_EQ_INT(_digits(10), 2);
        CHECK_EQ_INT(_digits(99), 2);
        CHECK_EQ_INT(_digits(100), 3);
        size_t v = 1000; int expect = 4;
        while (v <= 10000000000000000000ULL) {
            CHECK_EQ_INT(_digits(v), expect);
            if (v > UINT64_MAX / 10) break;
            v *= 10; expect++;
        }
        CHECK_EQ_INT(_digits(UINT64_MAX), 20);
    }

    TEST("__write_size_t writes decimal, returns end") {
        char buf[32];
        char *end = __write_size_t(buf, 0);
        *end = '\0';
        CHECK_STR_EQ(buf, "0");
        end = __write_size_t(buf, 12345);
        *end = '\0';
        CHECK_STR_EQ(buf, "12345");
        end = __write_size_t(buf, 1000000);
        *end = '\0';
        CHECK_STR_EQ(buf, "1000000");
    }

    TEST("itoa: 1-digit, 2-digit, loop, and max") {
        char buf[24];
        CHECK_STR_EQ(itoa(0, buf), "0");
        CHECK_STR_EQ(itoa(7, buf), "7");
        CHECK_STR_EQ(itoa(42, buf), "42");         /* final 2-digit branch */
        CHECK_STR_EQ(itoa(12345, buf), "12345");   /* 2-at-a-time loop */
        CHECK_STR_EQ(itoa(100, buf), "100");
        CHECK_STR_EQ(itoa(UINT64_MAX, buf), "18446744073709551615");
    }

    TEST("_append_len writes \"$N\\r\\n\"") {
        struct Buffer b = {0};
        b.data = malloc(64); b.capacity = 64; b.max_capacity = 64; b.used = 0; b.read_idx = 0;
        _append_len(&b, 4);
        CHECK_MEM_EQ(b.data, b.used, "$4\r\n");
        b.used = 0;
        _append_len(&b, 1048576);
        CHECK_MEM_EQ(b.data, b.used, "$1048576\r\n");
        free(b.data);
    }

    TF_SUITE_END();
}
