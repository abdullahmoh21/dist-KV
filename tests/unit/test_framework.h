#ifndef TEST_FRAMEWORK_H
#define TEST_FRAMEWORK_H

/*
 * Minimal single-header unit-test framework for dist-KV.
 *
 * Contract:
 *   - Each test suite lives in its own translation unit and exposes exactly one
 *     entry point: `int run_<name>_tests(void)` which returns the number of
 *     FAILED checks in that suite (0 == all passed).
 *   - Inside a suite, use TEST("desc") { ... } blocks and the CHECK_* macros.
 *   - unit_main.c calls every run_*_tests() and sums the failures.
 *
 * The macros keep running after a failed check (they don't abort) so one bad
 * assertion doesn't hide the rest of the suite — better signal when iterating
 * toward a coverage target.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

/* Per-suite counters. Each suite file gets its own copy (static). */
static int _tf_fails;
static int _tf_checks;
static const char *_tf_current;

#define TF_SUITE_BEGIN()   do { _tf_fails = 0; _tf_checks = 0; _tf_current = ""; } while (0)
#define TF_SUITE_END()     return _tf_fails

/* Label the current logical test. Purely for readable output. */
#define TEST(desc) _tf_current = (desc);

#define _TF_PASS() do { _tf_checks++; } while (0)
#define _TF_FAIL(fmt, ...) do { \
    _tf_checks++; _tf_fails++; \
    fprintf(stderr, "  FAIL [%s] " fmt " (%s:%d)\n", _tf_current, ##__VA_ARGS__, __FILE__, __LINE__); \
} while (0)

#define CHECK(cond) do { \
    if (cond) { _TF_PASS(); } else { _TF_FAIL("CHECK(%s)", #cond); } \
} while (0)

#define CHECK_MSG(cond, msg) do { \
    if (cond) { _TF_PASS(); } else { _TF_FAIL("%s", msg); } \
} while (0)

#define CHECK_EQ_INT(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a == _b) { _TF_PASS(); } \
    else { _TF_FAIL("expected %lld == %lld", _a, _b); } \
} while (0)

#define CHECK_EQ_DBL(a, b) do { \
    double _a = (double)(a), _b = (double)(b); \
    if (fabs(_a - _b) < 1e-9) { _TF_PASS(); } \
    else { _TF_FAIL("expected %g == %g", _a, _b); } \
} while (0)

#define CHECK_STR_EQ(a, b) do { \
    const char *_a = (a), *_b = (b); \
    if (_a && _b && strcmp(_a, _b) == 0) { _TF_PASS(); } \
    else { _TF_FAIL("expected \"%s\" == \"%s\"", _a ? _a : "(null)", _b ? _b : "(null)"); } \
} while (0)

/* Compare a non-null-terminated region against a C string literal. */
#define CHECK_MEM_EQ(ptr, len, lit) do { \
    size_t _l = (len); \
    if (_l == (sizeof(lit) - 1) && memcmp((ptr), (lit), _l) == 0) { _TF_PASS(); } \
    else { _TF_FAIL("mem mismatch vs \"%s\" (got len=%zu)", (lit), _l); } \
} while (0)

#define CHECK_NULL(p)     CHECK((p) == NULL)
#define CHECK_NOT_NULL(p) CHECK((p) != NULL)

#endif /* TEST_FRAMEWORK_H */
