#include "test_framework.h"
#include "utils/fast_parse.h"

/* fast_strtod: 0 on success (*out set), -1 on error. Fast paths for ints,
 * simple decimals, and infinities; rejects NaN and trailing garbage. */
static int fp(const char *s, double *out) {
    return fast_strtod(s, strlen(s), out);
}

int run_fastparse_tests(void) {
    TF_SUITE_BEGIN();
    double d;

    TEST("positive integer") {
        CHECK_EQ_INT(fp("42", &d), 0);
        CHECK_EQ_DBL(d, 42.0);
    }
    TEST("negative integer") {
        CHECK_EQ_INT(fp("-7", &d), 0);
        CHECK_EQ_DBL(d, -7.0);
    }
    TEST("zero") {
        CHECK_EQ_INT(fp("0", &d), 0);
        CHECK_EQ_DBL(d, 0.0);
    }
    TEST("large integer") {
        CHECK_EQ_INT(fp("1000000", &d), 0);
        CHECK_EQ_DBL(d, 1000000.0);
    }
    TEST("simple decimal") {
        CHECK_EQ_INT(fp("3.14", &d), 0);
        CHECK_EQ_DBL(d, 3.14);
    }
    TEST("negative decimal") {
        CHECK_EQ_INT(fp("-0.5", &d), 0);
        CHECK_EQ_DBL(d, -0.5);
    }
    TEST("leading plus") {
        CHECK_EQ_INT(fp("+42", &d), 0);
        CHECK_EQ_DBL(d, 42.0);
    }
    TEST("infinity forms") {
        CHECK_EQ_INT(fp("inf", &d), 0);
        CHECK(d > 1e300);
        CHECK_EQ_INT(fp("+inf", &d), 0);
        CHECK(d > 1e300);
        CHECK_EQ_INT(fp("-inf", &d), 0);
        CHECK(d < -1e300);
        CHECK_EQ_INT(fp("INF", &d), 0);   /* case-insensitive */
        CHECK(d > 1e300);
    }
    TEST("scientific notation (strtod fallback)") {
        CHECK_EQ_INT(fp("1.5e10", &d), 0);
        CHECK_EQ_DBL(d, 1.5e10);
    }
    TEST("NaN rejected") {
        CHECK_EQ_INT(fp("nan", &d), -1);
    }
    TEST("empty string rejected") {
        CHECK_EQ_INT(fp("", &d), -1);
    }
    TEST("trailing garbage rejected") {
        CHECK_EQ_INT(fp("42x", &d), -1);
        CHECK_EQ_INT(fp("3.14abc", &d), -1);
    }
    TEST("lone sign / dot rejected") {
        CHECK_EQ_INT(fp("-", &d), -1);
        CHECK_EQ_INT(fp("+", &d), -1);
    }
    TEST("many significant digits") {
        CHECK_EQ_INT(fp("123456789012345", &d), 0);
        CHECK_EQ_DBL(d, 123456789012345.0);
    }

    TF_SUITE_END();
}
