#include "utils/fast_parse.h"
#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Powers of 10 used for decimal scaling.
// Index i gives 10^i exactly as a double for i in [0, 18].
static const double pow10_table[19] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
};

int fast_strtod(const char *s, size_t len, double *out) {
    if (len == 0) return -1;

    const char *p   = s;
    const char *end = s + len;

    // --- Sign ---
    int negative = 0;
    if (*p == '-')      { negative = 1; p++; }
    else if (*p == '+') { p++; }

    if (p >= end) return -1;

    // --- Infinity (Redis accepts +inf / -inf as sorted-set sentinels) ---
    if (*p == 'i' || *p == 'I') {
        if ((end - p) >= 3 &&
            (p[1] == 'n' || p[1] == 'N') &&
            (p[2] == 'f' || p[2] == 'F')) {
            *out = negative ? -INFINITY : INFINITY;
            return 0;
        }
        return -1;
    }

    // --- NaN is not a valid ZADD score in Redis ---
    if (*p == 'n' || *p == 'N') return -1;

    // --- Integer part ---
    uint64_t int_part   = 0;
    int      int_digits = 0;

    while (p < end && (unsigned char)(*p - '0') <= 9) {
        if (int_digits >= 18) goto fallback;   // overflow risk, delegate to strtod
        int_part = int_part * 10 + (uint64_t)(*p - '0');
        int_digits++;
        p++;
    }

    // Pure integer — most common case for ZADD benchmark scores
    if (p == end) {
        if (int_digits == 0) return -1;
        *out = negative ? -(double)int_part : (double)int_part;
        return 0;
    }

    // --- Decimal part ---
    uint64_t frac_part   = 0;
    int      frac_digits = 0;

    if (*p == '.') {
        p++;
        while (p < end && (unsigned char)(*p - '0') <= 9) {
            // More than 15 significant digits risks precision errors; fall back.
            if (int_digits + frac_digits >= 15) goto fallback;
            frac_part = frac_part * 10 + (uint64_t)(*p - '0');
            frac_digits++;
            p++;
        }
    }

    // Scientific notation — hand off to strtod
    if (p < end && (*p == 'e' || *p == 'E')) goto fallback;

    // Trailing garbage
    if (p != end) return -1;

    // Neither integer digits nor fractional digits — invalid ("." alone)
    if (int_digits == 0 && frac_digits == 0) return -1;

    {
        double val = (double)int_part;
        if (frac_digits > 0) {
            val += (double)frac_part / pow10_table[frac_digits];
        }
        *out = negative ? -val : val;
        return 0;
    }

fallback:
    {
        // strtod requires a null-terminated string.
        // Any real double fits in 64 chars; longer strings are malformed.
        char buf[64];
        if (len >= sizeof(buf)) return -1;
        memcpy(buf, s, len);
        buf[len] = '\0';

        char *ep;
        errno   = 0;
        double d = strtod(buf, &ep);

        if (ep != buf + len) return -1;   // trailing garbage
        if (d != d)          return -1;   // NaN guard (belt-and-suspenders)
        *out = d;
        return 0;
    }
}
