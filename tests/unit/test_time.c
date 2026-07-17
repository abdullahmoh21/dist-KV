#include "test_framework.h"
#include "utils/time.h"
#include <stdint.h>

/* monotonic_ms is a steady clock; wallclock_ms is Unix-epoch ms (anchors TTLs). */
int run_time_tests(void) {
    TF_SUITE_BEGIN();

    TEST("monotonic non-zero and non-decreasing") {
        uint64_t a = monotonic_ms();
        CHECK(a > 0);
        uint64_t b = monotonic_ms();
        CHECK(b >= a);
    }

    TEST("wallclock is a plausible epoch-ms value") {
        uint64_t w = wallclock_ms();
        /* After 2020-01-01 (1.577e12 ms) and before year ~2100 (4.1e12 ms). */
        CHECK(w > 1577836800000ULL);
        CHECK(w < 4102444800000ULL);
    }

    TEST("wallclock advances or holds across calls") {
        uint64_t a = wallclock_ms();
        uint64_t b = wallclock_ms();
        CHECK(b >= a);
    }

    TF_SUITE_END();
}
