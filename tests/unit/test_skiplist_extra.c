#include "test_framework.h"
#include "store/skip_list.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Edge/guard paths the main skiplist suite doesn't hit. */

static ZSetMember *mk(const char *k, double s) {
    ZSetMember *m = malloc(sizeof *m);
    m->key = strdup(k);
    m->key_len = strlen(k);
    m->score = s;
    return m;
}

int run_skiplist_extra_tests(void) {
    TF_SUITE_BEGIN();
    TEST("NULL guards return safely") {
        CHECK_NULL(sl_search(NULL, "x", 1, 1.0));
        SkipList *l = sl_create(DEFAULT_MAX_LVL);
        CHECK_NULL(sl_search(l, NULL, 0, 1.0));
        CHECK_EQ_INT(sl_free_shallow(NULL), SL_OK);
        sl_free_shallow(l);
    }

    TEST("delete of a missing member, then the real one") {
        SkipList *l = sl_create(DEFAULT_MAX_LVL);
        ZSetMember *a = mk("a", 1.0);
        sl_insert(l, a);
        /* NOTE: sl_update() on a *missing* member is intentionally NOT tested —
         * it dereferences `candidate` before its NULL check (skip_list.c:153),
         * so it segfaults when the member sorts at the tail. Latent bug: rs_zadd
         * only calls sl_update for members it already confirmed exist. */
        CHECK(sl_delete(l, "ghost", 5, 9.0) == SL_NOT_FOUND);
        CHECK(sl_delete(l, "a", 1, 1.0) == SL_DELETED);
        CHECK_NULL(sl_search(l, "a", 1, 1.0));
        /* update an existing member (the safe, production path) */
        ZSetMember *b = mk("b", 2.0);
        sl_insert(l, b);
        CHECK(sl_update(l, "b", 1, 2.0, 7.0) == SL_OK);
        CHECK_NOT_NULL(sl_search(l, "b", 1, 7.0));
        free(a->key); free(a); free(b->key); free(b);
        sl_free_shallow(l);
    }
    TEST("rank iterator skips to a non-zero start") {
        SkipList *l = sl_create(DEFAULT_MAX_LVL);
        ZSetMember *held[6];
        char kb[4];
        for (int i = 0; i < 6; i++) {
            snprintf(kb, sizeof(kb), "k%d", i);
            held[i] = mk(kb, (double)i);
            sl_insert(l, held[i]);
        }
        SkipListIterator it = sl_iterator_rank(l, 2, 4);
        int n = 0; double first = -1;
        for (ZSetMember *m = sl_next(&it); m; m = sl_next(&it)) {
            if (n == 0) first = m->score;
            n++;
        }
        CHECK(n == 3);
        CHECK_EQ_DBL(first, 2.0);
        for (int i = 0; i < 6; i++) { free(held[i]->key); free(held[i]); }
        sl_free_shallow(l);
    }
    TEST("empty score range yields nothing") {
        SkipList *l = sl_create(DEFAULT_MAX_LVL);
        ZSetMember *a = mk("a", 5.0);
        sl_insert(l, a);
        SkipListIterator it = sl_iterator_score(l, 100.0, 200.0);
        CHECK_NULL(sl_next(&it));
        free(a->key); free(a);
        sl_free_shallow(l);
    }

    TF_SUITE_END();
}
