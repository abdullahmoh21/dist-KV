#ifndef FAST_PARSE_H
#define FAST_PARSE_H

#include <stddef.h>

// Parse a double from a non-null-terminated string of exactly `len` bytes.
//
// Fast paths (no memcpy, no locale, no strtod):
//   - Pure integers:       "42", "-7", "1000000"
//   - Simple decimals:     "3.14", "-0.5"  (up to 15 significant digits total)
//   - Infinities:          "inf", "+inf", "-inf" (case-insensitive)
//
// Fallback to strtod for: scientific notation ("1.5e10"), very long numbers.
//
// NaN is rejected (returns -1). Trailing garbage returns -1.
//
// Returns 0 on success, -1 on error.
int fast_strtod(const char *s, size_t len, double *out);

#endif
