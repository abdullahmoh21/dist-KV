#include "test_framework.h"
#include "utils/fast_format.h"
#include <inttypes.h>

/* fmt_uint writes decimal (no NUL, returns byte count); fmt_bulk_hdr writes "$<n>\r\n". */
int run_fastformat_tests(void) {
    TF_SUITE_BEGIN();
    char buf[64];

    TEST("fmt_uint zero") {
        int n = fmt_uint(buf, 0);
        CHECK_EQ_INT(n, 1);
        CHECK_MEM_EQ(buf, (size_t)n, "0");
    }

    TEST("fmt_uint single digit") {
        int n = fmt_uint(buf, 7);
        CHECK_MEM_EQ(buf, (size_t)n, "7");
    }

    TEST("fmt_uint multi digit") {
        int n = fmt_uint(buf, 12345);
        CHECK_MEM_EQ(buf, (size_t)n, "12345");
    }

    TEST("fmt_uint power-of-ten boundary") {
        int n = fmt_uint(buf, 1000000);
        CHECK_MEM_EQ(buf, (size_t)n, "1000000");
    }

    TEST("fmt_uint uint64 max") {
        int n = fmt_uint(buf, UINT64_MAX);
        CHECK_MEM_EQ(buf, (size_t)n, "18446744073709551615");
        CHECK_EQ_INT(n, 20);
    }

    TEST("fmt_uint round-trips via strtoull") {
        uint64_t vals[] = {1, 9, 10, 99, 100, 4294967296ULL, 9999999999ULL};
        for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); i++) {
            int n = fmt_uint(buf, vals[i]);
            buf[n] = '\0';
            CHECK_EQ_INT(strtoull(buf, NULL, 10), vals[i]);
        }
    }

    TEST("fmt_bulk_hdr small") {
        int n = fmt_bulk_hdr(buf, 4);
        CHECK_MEM_EQ(buf, (size_t)n, "$4\r\n");
    }

    TEST("fmt_bulk_hdr zero length") {
        int n = fmt_bulk_hdr(buf, 0);
        CHECK_MEM_EQ(buf, (size_t)n, "$0\r\n");
    }

    TEST("fmt_bulk_hdr large length") {
        int n = fmt_bulk_hdr(buf, 1048576);
        CHECK_MEM_EQ(buf, (size_t)n, "$1048576\r\n");
    }

    TF_SUITE_END();
}
