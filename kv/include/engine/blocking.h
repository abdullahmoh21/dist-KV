#ifndef BLOCKING_H
#define BLOCKING_H

#include <stddef.h>

// Forward declarations
typedef struct BulkString BulkString;
typedef struct RedisStore RedisStore;

// Try to serve one BZPOPMIN over `keys` (scanned left to right, first non-empty
// zset wins). On success the 3-element reply [key, member, score] is written to
// clientfd, the member is removed, and an emptied zset key is deleted.
//
// Returns:
//    1  served. *out_frame / *out_len point at a rewritten `ZPOPMIN <key>` RESP
//       frame that the CALLER must propagate (aof_add + repl_propagate). The
//       raw `BZPOPMIN ... <timeout>` frame must never be propagated: replaying
//       it would park a phantom client during AOF load, which aof_load treats
//       as a fatal error. Storage is a static scratch buffer, valid only until
//       the next call — every caller consumes it immediately.
//    0  nothing available on any key; caller should park the client.
//   -1  error (WRONGTYPE); the error reply has already been sent.
int zset_serve_blocking_pop(int clientfd, RedisStore *store,
                            const BulkString *keys, int nkeys,
                            const char **out_frame, size_t *out_len);

#endif
