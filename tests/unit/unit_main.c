/*
 * Unit test runner. Each suite returns its failure count; we sum them and
 * exit non-zero if anything failed. Add new suites by declaring the entry
 * point here and appending it to SUITES.
 */
#include <stdio.h>

int run_fastformat_tests(void);
int run_fastparse_tests(void);
int run_time_tests(void);
int run_buffer_tests(void);
int run_hashmap_tests(void);
int run_skiplist_tests(void);
int run_skiplist_extra_tests(void);
int run_resp_parser_tests(void);
int run_redis_store_tests(void);
int run_engine_tests(void);
int run_reply_tests(void);
int run_aof_compact_tests(void);
int run_aof_manager_tests(void);
int run_aof_encode_tests(void);

struct suite { const char *name; int (*fn)(void); };

int main(void) {
    struct suite suites[] = {
        {"fastformat",   run_fastformat_tests},
        {"fastparse",    run_fastparse_tests},
        {"time",         run_time_tests},
        {"buffer",       run_buffer_tests},
        {"hashmap",      run_hashmap_tests},
        {"skiplist",     run_skiplist_tests},
        {"skiplist_x",   run_skiplist_extra_tests},
        {"resp_parser",  run_resp_parser_tests},
        {"redis_store",  run_redis_store_tests},
        {"engine",       run_engine_tests},
        {"reply",        run_reply_tests},
        {"aof_compact",  run_aof_compact_tests},
        {"aof_manager",  run_aof_manager_tests},
        {"aof_encode",   run_aof_encode_tests},
    };
    int total = 0;
    size_t n = sizeof(suites) / sizeof(suites[0]);
    for (size_t i = 0; i < n; i++) {
        int f = suites[i].fn();
        printf("%-14s %s\n", suites[i].name, f == 0 ? "ok" : "FAILED");
        fflush(stdout);
        total += f;
    }
    printf("\nunit: %s (%d failing checks)\n", total == 0 ? "ALL PASS" : "FAILURES", total);
    return total == 0 ? 0 : 1;
}
