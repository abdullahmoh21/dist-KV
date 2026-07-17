#include "test_framework.h"
#include "aof/aof.h"
#include "parser/resp_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

/*
 * Drives the double-buffer async AOF writer end to end in-process:
 *   create -> add enough bytes to force a buffer swap (background thread drains
 *   the standby buffer) -> force_flush -> check_flush timer -> redirect the fd
 *   -> clean destroy (the shutdown/join path that a SIGINT'd server skips).
 * aof_create() opens "appendonly.aof" in the cwd, so we run from a temp dir.
 */

static char g_frame[512];
static RedisCommand g_cmd;

static void build_set(void) {
    /* SET akey <200 bytes> */
    char val[201];
    memset(val, 'A', 200); val[200] = '\0';
    int off = snprintf(g_frame, sizeof(g_frame),
                       "*3\r\n$3\r\nSET\r\n$4\r\nakey\r\n$200\r\n%s\r\n", val);
    parse_array_command(g_frame, (size_t)off, &g_cmd);
}

int run_aof_manager_tests(void) {
    TF_SUITE_BEGIN();

    char oldcwd[4096];
    if (!getcwd(oldcwd, sizeof(oldcwd))) { _TF_FAIL("getcwd"); TF_SUITE_END(); }
    const char *tmp = "/tmp/dkv_aofmgr_test";
    mkdir(tmp, 0755);
    if (chdir(tmp) != 0) { _TF_FAIL("chdir tmp"); TF_SUITE_END(); }
    unlink("appendonly.aof");
    unlink("redirected.aof");

    build_set();

    AOFManager *aof = NULL;
    TEST("aof_create starts the manager + writer thread") {
        CHECK_EQ_INT(aof_create(&aof), AOF_OK);
        CHECK_NOT_NULL(aof);
    }

    TEST("aof_add many commands forces at least one buffer swap") {
        /* ~230 bytes/command * 6000 = ~1.38MB > 1MB active buffer -> swap */
        int ok = 1;
        for (int i = 0; i < 6000; i++) {
            if (aof_add(aof, &g_cmd) != AOF_OK) { ok = 0; break; }
        }
        CHECK(ok);
    }

    TEST("force + timer flush paths") {
        CHECK_EQ_INT(aof_force_flush(aof), AOF_OK);
        /* <1s since last flush -> early-return branch */
        CHECK_EQ_INT(aof_check_flush(aof), AOF_OK);
    }

    TEST("redirect the AOF fd, then keep writing") {
        int fd = open("redirected.aof", O_WRONLY | O_CREAT | O_APPEND, 0644);
        CHECK(fd >= 0);
        CHECK_EQ_INT(aof_redirect(aof, fd), AOF_OK);
        CHECK_EQ_INT(aof_add(aof, &g_cmd), AOF_OK);
        CHECK_EQ_INT(aof_force_flush(aof), AOF_OK);
    }

    TEST("appendonly.aof received data") {
        struct stat st;
        CHECK(stat("appendonly.aof", &st) == 0 && st.st_size > 0);
    }

    /* clean shutdown: joins the writer thread + frees everything */
    aof_destroy(aof);
    free_command(&g_cmd);

    unlink("appendonly.aof");
    unlink("redirected.aof");
    if (chdir(oldcwd) != 0) { _TF_FAIL("chdir back"); }

    TF_SUITE_END();
}
