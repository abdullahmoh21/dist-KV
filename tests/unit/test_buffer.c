#include "test_framework.h"
#include "store/buffer.h"
#include <stdlib.h>

/* expand_buffer_to / expand_buffer return 1 on success, 0 on no-op/failure.
 * They realloc buff->data (NULL-safe start) and grow capacity toward max_capacity. */
int run_buffer_tests(void) {
    TF_SUITE_BEGIN();

    TEST("doubling growth from a base capacity") {
        struct Buffer b = {0};
        b.data = NULL; b.capacity = 4096; b.max_capacity = 1 << 20;
        /* pre-allocate so realloc has a valid block to grow */
        b.data = malloc(b.capacity);
        int r = expand_buffer(&b);
        CHECK_EQ_INT(r, 1);
        CHECK_EQ_INT(b.capacity, 8192);
        free(b.data);
    }

    TEST("expand_to jumps straight to min_needed when bigger than double") {
        struct Buffer b = {0};
        b.capacity = 100; b.max_capacity = 1 << 20;
        b.data = malloc(b.capacity);
        int r = expand_buffer_to(&b, 5000);
        CHECK_EQ_INT(r, 1);
        CHECK_EQ_INT(b.capacity, 5000);   /* 5000 > 2*100, so min_needed wins */
        free(b.data);
    }

    TEST("no-op when already at max capacity") {
        struct Buffer b = {0};
        b.capacity = 1 << 20; b.max_capacity = 1 << 20;
        b.data = malloc(16);
        int r = expand_buffer(&b);
        CHECK_EQ_INT(r, 0);               /* capacity >= max_capacity short-circuit */
        CHECK_EQ_INT(b.capacity, 1 << 20);
        free(b.data);
    }

    TEST("min_needed above max_capacity is rejected") {
        struct Buffer b = {0};
        b.capacity = 100; b.max_capacity = 200;
        b.data = malloc(b.capacity);
        int r = expand_buffer_to(&b, 500);   /* clamps to 200, still < 500 -> 0 */
        CHECK_EQ_INT(r, 0);
        CHECK_EQ_INT(b.capacity, 100);       /* unchanged */
        free(b.data);
    }

    TEST("expand_buffer near max fails when double exceeds max") {
        struct Buffer b = {0};
        b.capacity = 600; b.max_capacity = 1000;
        b.data = malloc(b.capacity);
        /* expand_buffer asks for min_needed = 1200; new_size clamps to 1000 < 1200 -> 0 */
        int r = expand_buffer(&b);
        CHECK_EQ_INT(r, 0);
        free(b.data);
    }

    TEST("growth to exactly max is allowed") {
        struct Buffer b = {0};
        b.capacity = 500; b.max_capacity = 1000;
        b.data = malloc(b.capacity);
        int r = expand_buffer_to(&b, 1000);  /* new_size=1000 == max, ok */
        CHECK_EQ_INT(r, 1);
        CHECK_EQ_INT(b.capacity, 1000);
        free(b.data);
    }

    TF_SUITE_END();
}
