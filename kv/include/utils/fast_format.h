#ifndef FAST_FORMAT_H
#define FAST_FORMAT_H

#include <stdint.h>
#include <stddef.h>

// Writes the decimal representation of n directly into buf (no null terminator).
// buf must have at least 20 bytes available.
// Returns the number of bytes written.
int fmt_uint(char *buf, uint64_t n);

// Writes "$<n>\r\n" into buf.
// buf must have at least 23 bytes available.
// Returns the number of bytes written.
int fmt_bulk_hdr(char *buf, size_t n);

#endif
