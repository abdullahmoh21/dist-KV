#include "aof_internal.h"

int _digits(size_t n) {
    if (n < 10) return 1;
    if (n < 100) return 2;
    if (n < 1000) return 3;
    if (n < 10000) return 4;
    if (n < 100000) return 5;
    if (n < 1000000) return 6;
    if (n < 10000000) return 7;
    if (n < 100000000) return 8;
    if (n < 1000000000) return 9;
    if (n < 10000000000ULL) return 10;
    if (n < 100000000000ULL) return 11;
    if (n < 1000000000000ULL) return 12;
    if (n < 10000000000000ULL) return 13;
    if (n < 100000000000000ULL) return 14;
    if (n < 1000000000000000ULL) return 15;
    if (n < 10000000000000000ULL) return 16;
    if (n < 100000000000000000ULL) return 17;
    if (n < 1000000000000000000ULL) return 18;
    if (n < 10000000000000000000ULL) return 19;
    return 20;
}

char* __write_size_t(char *dest, size_t n) {
    int d = _digits(n);
    char *p = dest + d;
    char *digit_ptr = p - 1;

    // Fill backwards
    if (n == 0) {
        *digit_ptr = '0';
    } else {
        while (n > 0) {
            *digit_ptr-- = (n % 10) + '0';
            n /= 10;
        }
    }
    
    return p;
}

static const char digits[201] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

// cracked AI-gen version
char* itoa(uint64_t val, char* buf) {
    char* p = buf + 20; // End of a 21-byte buffer
    *p = '\0';

    // Process two digits at a time using the lookup table
    while (val >= 100) {
        const uint32_t intermediate = (uint32_t)(val % 100);
        val /= 100;
        const uint32_t idx = intermediate * 2;
        *--p = digits[idx + 1];
        *--p = digits[idx];
    }

    // Handle the final 1 or 2 digits
    if (val >= 10) {
        const uint32_t idx = (uint32_t)val * 2;
        *--p = digits[idx + 1];
        *--p = digits[idx];
    } else {
        *--p = (char)('0' + val);
    }

    return p;
}

// Helper to write "$<len>\r\n" directly into the buffer
void _append_len(struct Buffer *buf, size_t len) {
    buf->data[buf->used++] = '$';
    
    char *end_of_window = buf->data + buf->used + 20; 
    char *start = itoa((uint64_t)len, end_of_window);
    
    size_t digit_count = (size_t)(end_of_window - start);
    
    memmove(buf->data + buf->used, start, digit_count);
    buf->used += digit_count;

    buf->data[buf->used++] = '\r';
    buf->data[buf->used++] = '\n';
}