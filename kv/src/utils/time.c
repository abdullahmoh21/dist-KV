
#include <stdint.h>
#include <time.h>

uint64_t monotonic_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (uint64_t)ts.tv_sec * 1000 +
           ts.tv_nsec / 1000000;
}

// Wall-clock (Unix-epoch) milliseconds. TTL deadlines MUST use this, not
// monotonic_ms(): a persisted PEXPIREAT stores an absolute deadline that has to
// mean the same instant across an AOF restart and on a replica running on
// another machine. CLOCK_MONOTONIC resets on reboot and has an arbitrary epoch,
// so it can't anchor a durable/replicated deadline; CLOCK_REALTIME can (assumes
// NTP-synced clocks, same assumption real Redis makes).
uint64_t wallclock_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    return (uint64_t)ts.tv_sec * 1000 +
           ts.tv_nsec / 1000000;
}
