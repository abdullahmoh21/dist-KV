#ifndef AOF_INTERNAL_H
#define AOF_INTERNAL_H

#include <aof/aof.h>
#include <store/redis_store.h>
#include <store/hashmap.h>
#include <store/buffer.h>
#include <parser/resp_parser.h>
#include <engine/execution_engine.h>
#include "utils/time.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>

int _digits(size_t n);
char* __write_size_t(char *dest, size_t n);
char* itoa(uint64_t val, char* buf);
void _append_len(struct Buffer *buf, size_t len);

#endif