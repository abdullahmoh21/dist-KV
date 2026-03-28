#include "utils/fast_format.h"

// Two-digit lookup table: index i*2 and i*2+1 give the two ASCII digits of i (00-99)
static const char two_digits[201] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

// Returns the number of decimal digits in n (n=0 counts as 1 digit).
static int _count_digits(uint64_t n) {
    if (n < 10ULL)                  return 1;
    if (n < 100ULL)                 return 2;
    if (n < 1000ULL)                return 3;
    if (n < 10000ULL)               return 4;
    if (n < 100000ULL)              return 5;
    if (n < 1000000ULL)             return 6;
    if (n < 10000000ULL)            return 7;
    if (n < 100000000ULL)           return 8;
    if (n < 1000000000ULL)          return 9;
    if (n < 10000000000ULL)         return 10;
    if (n < 100000000000ULL)        return 11;
    if (n < 1000000000000ULL)       return 12;
    if (n < 10000000000000ULL)      return 13;
    if (n < 100000000000000ULL)     return 14;
    if (n < 1000000000000000ULL)    return 15;
    if (n < 10000000000000000ULL)   return 16;
    if (n < 100000000000000000ULL)  return 17;
    if (n < 1000000000000000000ULL) return 18;
    return 20;
}

int fmt_uint(char *buf, uint64_t n) {
    int d = _count_digits(n);
    // Fill digits right-to-left within the reserved d-byte window, two at a time.
    char *p = buf + d - 1;

    while (n >= 100) {
        uint32_t idx = (uint32_t)(n % 100) * 2;
        n /= 100;
        *p-- = two_digits[idx + 1];
        *p-- = two_digits[idx];
    }
    if (n >= 10) {
        uint32_t idx = (uint32_t)n * 2;
        *p-- = two_digits[idx + 1];
        *p   = two_digits[idx];
    } else {
        *p = (char)('0' + n);
    }
    return d;
}

int fmt_bulk_hdr(char *buf, size_t n) {
    buf[0] = '$';
    int d = fmt_uint(buf + 1, (uint64_t)n);
    buf[1 + d] = '\r';
    buf[2 + d] = '\n';
    return 3 + d;  // '$' + digits + "\r\n"
}
