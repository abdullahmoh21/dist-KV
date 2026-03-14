
#include <stdint.h>
#include <time.h>

uint64_t monotonic_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (uint64_t)ts.tv_sec * 1000 +
           ts.tv_nsec / 1000000;
}