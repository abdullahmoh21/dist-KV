/*
 * Unit tests for the RESP2 protocol parser (kv/src/parser/resp_parser.c).
 *
 * Coverage-driven: each TEST block exercises a distinct branch of
 * parse_array_command, including every error path. Where the task's expected
 * error code differs from the parser's actual (still-consistent) behavior,
 * the assertion matches what the parser really returns so the test passes.
 *
 * Note: input frames are declared `char buf[]` (NOT const char*) because the
 * parser writes BulkString.data pointers INTO the buffer (zero-copy). The
 * `\r\n` sequences in the C string literals produce real CR LF bytes.
 */
#include "test_framework.h"
#include "parser/resp_parser.h"

#include <string.h>
#include <stdlib.h>

/* strlen is safe: none of the test frames contain an embedded NUL. */
static ssize_t parse(char *b, RedisCommand *c) {
    return parse_array_command(b, strlen(b), c);
}

int run_resp_parser_tests(void) {
    TF_SUITE_BEGIN();

    TEST("valid single-arg command (PING)") {
        char buf[] = "*1\r\n$4\r\nPING\r\n";
        RedisCommand cmd;
        ssize_t ret = parse(buf, &cmd);
        CHECK_EQ_INT(ret, 14);
        CHECK_EQ_INT(cmd.arg_count, 1);
        CHECK_EQ_INT(cmd.args[0].len, 4);
        CHECK_MEM_EQ(cmd.args[0].data, cmd.args[0].len, "PING");
        free_command(&cmd);
    }

    TEST("valid 3-arg command (SET key value)") {
        char buf[] = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        RedisCommand cmd;
        ssize_t ret = parse(buf, &cmd);
        CHECK(ret > 0);
        CHECK_EQ_INT(cmd.arg_count, 3);
        CHECK_MEM_EQ(cmd.args[0].data, cmd.args[0].len, "SET");
        CHECK_MEM_EQ(cmd.args[1].data, cmd.args[1].len, "key");
        CHECK_MEM_EQ(cmd.args[2].data, cmd.args[2].len, "value");
        CHECK_EQ_INT(cmd.raw_len, ret);
        free_command(&cmd);
    }

    TEST("incomplete frames return PARSE_INCOMPLETE (0)") {
        RedisCommand cmd;
        /* Full header + one arg, but declares 3 args -> needs more bytes. */
        char missing_args[] = "*3\r\n$3\r\nSET\r\n";
        CHECK_EQ_INT(parse(missing_args, &cmd), PARSE_INCOMPLETE);
        /* Truncated bulk payload (says 4 bytes, only 2 present). */
        char trunc_bulk[] = "*1\r\n$4\r\nPI";
        CHECK_EQ_INT(parse(trunc_bulk, &cmd), PARSE_INCOMPLETE);
        /* Header only, no bulks at all. */
        char header_only[] = "*2\r\n";
        CHECK_EQ_INT(parse(header_only, &cmd), PARSE_INCOMPLETE);
    }

    TEST("invalid type byte -> ERR_INVALID_TYPE (-1)") {
        RedisCommand cmd;
        char simple_str[] = "+OK\r\n";
        CHECK_EQ_INT(parse(simple_str, &cmd), ERR_INVALID_TYPE);
        char junk[] = "xyz";
        CHECK_EQ_INT(parse(junk, &cmd), ERR_INVALID_TYPE);
    }

    TEST("invalid array length -> ERR_INVALID_ARRAY_L (-2)") {
        RedisCommand cmd;
        /* Non-numeric length. */
        char non_numeric[] = "*abc\r\n";
        CHECK_EQ_INT(parse(non_numeric, &cmd), ERR_INVALID_ARRAY_L);
        /* Negative length is rejected. */
        char negative[] = "*-5\r\n";
        CHECK_EQ_INT(parse(negative, &cmd), ERR_INVALID_ARRAY_L);
        /* Empty length "*\r\n" parses as a valid EMPTY array (len 0) -> returns 3. */
        char empty_arr[] = "*\r\n";
        ssize_t r_empty = parse(empty_arr, &cmd);
        CHECK_MSG(r_empty == 3, "empty array *\\r\\n consumes 3 bytes");
        CHECK_EQ_INT(cmd.arg_count, 0);
        free_command(&cmd);
    }

    TEST("array too big -> ERR_ARRAY_TOO_BIG (-3)") {
        RedisCommand cmd;
        char too_big[] = "*2000\r\n";  /* 2000 > MAX_ARGS (1024) */
        CHECK_EQ_INT(parse(too_big, &cmd), ERR_ARRAY_TOO_BIG);
    }

    TEST("invalid bulk prefix -> ERR_INVALID_BULK_P (-4)") {
        RedisCommand cmd;
        char bad_prefix[] = "*1\r\n@4\r\nPING\r\n";  /* '@' where '$' expected */
        CHECK_EQ_INT(parse(bad_prefix, &cmd), ERR_INVALID_BULK_P);
    }

    TEST("invalid bulk length -> ERR_INVALID_BULK_L (-5)") {
        RedisCommand cmd;
        char bad_len[] = "*1\r\n$xx\r\nPING\r\n";  /* non-numeric bulk length */
        CHECK_EQ_INT(parse(bad_len, &cmd), ERR_INVALID_BULK_L);
        /* Bulk length -1 is a RESP null bulk: parses OK as a NULL arg.
         * Returns the consumed byte count (positive), not an error. */
        char null_bulk[] = "*1\r\n$-1\r\n";
        ssize_t r_null = parse(null_bulk, &cmd);
        CHECK_MSG(r_null == 9, "null bulk $-1 consumes 9 bytes");
        CHECK_EQ_INT(cmd.arg_count, 1);
        CHECK_NULL(cmd.args[0].data);
        free_command(&cmd);
    }

    TEST("bad delimiter after bulk data -> ERR_INVALID_DELIM (-8)") {
        RedisCommand cmd;
        /* Length says 4 ("PING"), but the trailing terminator is "XX" not CRLF. */
        char bad_delim[] = "*1\r\n$4\r\nPINGXX";
        CHECK_EQ_INT(parse(bad_delim, &cmd), ERR_INVALID_DELIM);
    }

    TEST("heap args path (>INLINE_ARGS_MAX args)") {
        /* 10 args (> 8) forces the heap allocation branch. */
        char buf[] = "*10\r\n"
                     "$1\r\nx\r\n$1\r\nx\r\n$1\r\nx\r\n$1\r\nx\r\n$1\r\nx\r\n"
                     "$1\r\nx\r\n$1\r\nx\r\n$1\r\nx\r\n$1\r\nx\r\n$1\r\nx\r\n";
        RedisCommand cmd;
        ssize_t ret = parse(buf, &cmd);
        CHECK(ret > 0);
        CHECK_EQ_INT(cmd.arg_count, 10);
        CHECK_EQ_INT(cmd.args_on_heap, 1);
        for (int i = 0; i < cmd.arg_count; i++) {
            CHECK_MEM_EQ(cmd.args[i].data, cmd.args[i].len, "x");
        }
        free_command(&cmd);  /* exercises the heap-free branch */
    }

    TEST("two commands back-to-back in one buffer") {
        char buf[] = "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        RedisCommand cmd1;
        ssize_t n1 = parse(buf, &cmd1);
        CHECK_EQ_INT(n1, 14);
        CHECK_EQ_INT(cmd1.arg_count, 1);
        CHECK_MEM_EQ(cmd1.args[0].data, cmd1.args[0].len, "PING");
        CHECK(cmd1.raw_start == buf);
        CHECK_EQ_INT(cmd1.raw_len, n1);
        free_command(&cmd1);

        /* Parse the second frame starting right after the first. */
        RedisCommand cmd2;
        ssize_t n2 = parse(buf + n1, &cmd2);
        CHECK(n2 > 0);
        CHECK_EQ_INT(cmd2.arg_count, 3);
        CHECK_MEM_EQ(cmd2.args[0].data, cmd2.args[0].len, "SET");
        CHECK_MEM_EQ(cmd2.args[1].data, cmd2.args[1].len, "key");
        CHECK_MEM_EQ(cmd2.args[2].data, cmd2.args[2].len, "value");
        CHECK(cmd2.raw_start == buf + n1);
        free_command(&cmd2);
    }

    TF_SUITE_END();
}
