#include "test_framework.h"
#include "store/skip_list.h"

#include <stdlib.h>
#include <string.h>

/*
 * Unit tests for the skip list (sorted-set backing structure).
 *
 * Ownership contract: sl_insert stores the ZSetMember pointer we pass; it does
 * NOT copy it. sl_free_shallow frees the list's nodes but NOT our ZSetMembers.
 * So every mk() allocation is tracked in `g_tracked` and freed at the very end,
 * after all sl_free_shallow calls.
 */

#define TRACK_CAP 512
static ZSetMember *g_tracked[TRACK_CAP];
static int g_ntracked;

/* Allocate a ZSetMember with a stable strdup'd key and register it for cleanup. */
static ZSetMember *mk(const char *k, double s) {
    ZSetMember *m = malloc(sizeof *m);
    m->key = strdup(k);
    m->key_len = strlen(k);
    m->score = s;
    if (g_ntracked < TRACK_CAP) {
        g_tracked[g_ntracked++] = m;
    }
    return m;
}

static void free_tracked(void) {
    for (int i = 0; i < g_ntracked; i++) {
        free(g_tracked[i]->key);
        free(g_tracked[i]);
    }
    g_ntracked = 0;
}

int run_skiplist_tests(void) {
    TF_SUITE_BEGIN();

    g_ntracked = 0;

    /* ---- 1. Empty list: iterator, search miss, delete miss ---------------- */
    TEST("empty list: rank iterator yields nothing, search/delete miss") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);
        CHECK_EQ_INT(list->size, 0);

        SkipListIterator rit = sl_iterator_rank(list, 0, 10);
        CHECK_NULL(sl_next(&rit));

        SkipListIterator sit = sl_iterator_score(list, -1e18, 1e18);
        CHECK_NULL(sl_next(&sit));

        CHECK_NULL(sl_search(list, "nope", 4, 1.0));

        SL_RESULT dr = sl_delete(list, "nope", 4, 1.0);
        CHECK_EQ_INT(dr, SL_NOT_FOUND);

        sl_free_shallow(list);
    }

    /* ---- 2. Insert distinct scores; search each returns the right ptr ----- */
    TEST("insert distinct scores; search returns correct member") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        /* a=1, b=2, c=3, d=5, e=4 (inserted out of score order on purpose) */
        ZSetMember *a = mk("a", 1.0);
        ZSetMember *b = mk("b", 2.0);
        ZSetMember *c = mk("c", 3.0);
        ZSetMember *d = mk("d", 5.0);
        ZSetMember *e = mk("e", 4.0);

        CHECK_EQ_INT(sl_insert(list, a), SL_OK);
        CHECK_EQ_INT(sl_insert(list, b), SL_OK);
        CHECK_EQ_INT(sl_insert(list, c), SL_OK);
        CHECK_EQ_INT(sl_insert(list, d), SL_OK);
        CHECK_EQ_INT(sl_insert(list, e), SL_OK);
        CHECK_EQ_INT(list->size, 5);

        ZSetMember *fa = sl_search(list, "a", 1, 1.0);
        CHECK_NOT_NULL(fa);
        CHECK(fa == a);
        CHECK_EQ_DBL(fa->score, 1.0);

        ZSetMember *fd = sl_search(list, "d", 1, 5.0);
        CHECK_NOT_NULL(fd);
        CHECK(fd == d);
        CHECK_EQ_DBL(fd->score, 5.0);

        ZSetMember *fe = sl_search(list, "e", 1, 4.0);
        CHECK_NOT_NULL(fe);
        CHECK(fe == e);
        CHECK_EQ_DBL(fe->score, 4.0);

        /* right key but wrong score -> miss (score participates in the lookup) */
        CHECK_NULL(sl_search(list, "a", 1, 99.0));

        sl_free_shallow(list);
    }

    /* ---- 3. Score-order iteration yields ascending scores ----------------- */
    TEST("full score iteration is non-decreasing") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        CHECK_EQ_INT(sl_insert(list, mk("a", 1.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("b", 2.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("c", 3.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("d", 5.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("e", 4.0)), SL_OK);

        SkipListIterator it = sl_iterator_score(list, -1e18, 1e18);
        double scores[16];
        int n = 0;
        ZSetMember *m;
        while ((m = sl_next(&it)) != NULL && n < 16) {
            scores[n++] = m->score;
        }
        CHECK_EQ_INT(n, 5);

        int ordered = 1;
        for (int i = 1; i < n; i++) {
            if (scores[i] < scores[i - 1]) ordered = 0;
        }
        CHECK_MSG(ordered, "scores must come out non-decreasing");
        /* explicit: 1,2,3,4,5 */
        CHECK_EQ_DBL(scores[0], 1.0);
        CHECK_EQ_DBL(scores[1], 2.0);
        CHECK_EQ_DBL(scores[2], 3.0);
        CHECK_EQ_DBL(scores[3], 4.0);
        CHECK_EQ_DBL(scores[4], 5.0);

        sl_free_shallow(list);
    }

    /* ---- 4. Range query over score interval [2,4] ------------------------ */
    TEST("score range [2,4] returns only in-range members") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        CHECK_EQ_INT(sl_insert(list, mk("a", 1.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("b", 2.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("c", 3.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("d", 5.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("e", 4.0)), SL_OK);

        SkipListIterator it = sl_iterator_score(list, 2.0, 4.0);
        int n = 0;
        int all_in_range = 1;
        ZSetMember *m;
        while ((m = sl_next(&it)) != NULL) {
            if (m->score < 2.0 || m->score > 4.0) all_in_range = 0;
            n++;
        }
        CHECK_EQ_INT(n, 3); /* scores 2, 3, 4 */
        CHECK_MSG(all_in_range, "every returned score must be within [2,4]");

        sl_free_shallow(list);
    }

    /* ---- 5. Rank iterator returns the lowest-score members in order ------- */
    TEST("rank range [0,2] returns 3 lowest members in order") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        CHECK_EQ_INT(sl_insert(list, mk("a", 1.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("b", 2.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("c", 3.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("d", 5.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("e", 4.0)), SL_OK);

        SkipListIterator it = sl_iterator_rank(list, 0, 2);
        ZSetMember *m0 = sl_next(&it);
        ZSetMember *m1 = sl_next(&it);
        ZSetMember *m2 = sl_next(&it);
        ZSetMember *m3 = sl_next(&it);

        CHECK_NOT_NULL(m0);
        CHECK_NOT_NULL(m1);
        CHECK_NOT_NULL(m2);
        CHECK_NULL(m3); /* only 3 requested */

        CHECK_STR_EQ(m0->key, "a");
        CHECK_STR_EQ(m1->key, "b");
        CHECK_STR_EQ(m2->key, "c");
        CHECK_EQ_DBL(m0->score, 1.0);
        CHECK_EQ_DBL(m1->score, 2.0);
        CHECK_EQ_DBL(m2->score, 3.0);

        sl_free_shallow(list);
    }

    /* ---- 6. sl_update moves a member to its new score position ----------- */
    TEST("update relocates member; old-score search misses, new-score hits") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        ZSetMember *a = mk("a", 1.0);
        ZSetMember *b = mk("b", 2.0);
        ZSetMember *e = mk("e", 4.0);
        CHECK_EQ_INT(sl_insert(list, a), SL_OK);
        CHECK_EQ_INT(sl_insert(list, b), SL_OK);
        CHECK_EQ_INT(sl_insert(list, e), SL_OK);

        /* move e from 4.0 to 10.0 (past everything -> becomes the tail) */
        SL_RESULT ur = sl_update(list, "e", 1, 4.0, 10.0);
        CHECK_EQ_INT(ur, SL_OK);

        CHECK_NULL(sl_search(list, "e", 1, 4.0));
        ZSetMember *fe = sl_search(list, "e", 1, 10.0);
        CHECK_NOT_NULL(fe);
        CHECK(fe == e);
        CHECK_EQ_DBL(fe->score, 10.0);

        /* iteration order now reflects the new position: a(1), b(2), e(10) */
        SkipListIterator it = sl_iterator_score(list, -1e18, 1e18);
        ZSetMember *x0 = sl_next(&it);
        ZSetMember *x1 = sl_next(&it);
        ZSetMember *x2 = sl_next(&it);
        CHECK_NOT_NULL(x0);
        CHECK_NOT_NULL(x1);
        CHECK_NOT_NULL(x2);
        CHECK_STR_EQ(x0->key, "a");
        CHECK_STR_EQ(x1->key, "b");
        CHECK_STR_EQ(x2->key, "e");
        CHECK_NULL(sl_next(&it));

        sl_free_shallow(list);
    }

    /* ---- 7. sl_delete removes a member; rest still iterate --------------- */
    TEST("delete existing member; remaining iterate correctly") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        CHECK_EQ_INT(sl_insert(list, mk("a", 1.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("b", 2.0)), SL_OK);
        CHECK_EQ_INT(sl_insert(list, mk("c", 3.0)), SL_OK);
        CHECK_EQ_INT(list->size, 3);

        /* delete the middle element */
        SL_RESULT dr = sl_delete(list, "b", 1, 2.0);
        CHECK_EQ_INT(dr, SL_DELETED);
        CHECK_EQ_INT(list->size, 2);

        CHECK_NULL(sl_search(list, "b", 1, 2.0));

        /* deleting again -> not found */
        CHECK_EQ_INT(sl_delete(list, "b", 1, 2.0), SL_NOT_FOUND);

        SkipListIterator it = sl_iterator_score(list, -1e18, 1e18);
        ZSetMember *r0 = sl_next(&it);
        ZSetMember *r1 = sl_next(&it);
        CHECK_NOT_NULL(r0);
        CHECK_NOT_NULL(r1);
        CHECK_STR_EQ(r0->key, "a");
        CHECK_STR_EQ(r1->key, "c");
        CHECK_NULL(sl_next(&it));

        sl_free_shallow(list);
    }

    /* ---- 8. Duplicate handling ------------------------------------------ */
    TEST("exact duplicate rejected; same key different score accepted") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        ZSetMember *a1 = mk("a", 1.0);
        CHECK_EQ_INT(sl_insert(list, a1), SL_OK);

        /* same key + same score -> SL_DUPLICATE, not stored (we still own a2) */
        ZSetMember *a2 = mk("a", 1.0);
        SL_RESULT dup = sl_insert(list, a2);
        CHECK_EQ_INT(dup, SL_DUPLICATE);
        CHECK_EQ_INT(list->size, 1);

        /* same key, different score -> distinct entry, accepted */
        ZSetMember *a3 = mk("a", 2.0);
        CHECK_EQ_INT(sl_insert(list, a3), SL_OK);
        CHECK_EQ_INT(list->size, 2);
        CHECK(sl_search(list, "a", 1, 1.0) == a1);
        CHECK(sl_search(list, "a", 1, 2.0) == a3);

        /* invalid inserts */
        CHECK_EQ_INT(sl_insert(list, NULL), SL_ERR);
        CHECK_EQ_INT(sl_insert(NULL, a1), SL_ERR);

        sl_free_shallow(list);
    }

    /* ---- 9. Larger insert forces multiple levels; full order holds ------- */
    TEST("60 distinct members iterate in non-decreasing score order") {
        SkipList *list = sl_create(DEFAULT_MAX_LVL);
        CHECK_NOT_NULL(list);

        const int N = 60;
        for (int i = 0; i < N; i++) {
            /* deterministic distinct scores: 37 and 101 are coprime */
            double score = (double)((i * 37) % 101);
            char key[16];
            snprintf(key, sizeof key, "m%d", i);
            CHECK_EQ_INT(sl_insert(list, mk(key, score)), SL_OK);
        }
        CHECK_EQ_INT(list->size, N);

        SkipListIterator it = sl_iterator_score(list, -1e18, 1e18);
        int n = 0;
        int ordered = 1;
        double prev = -1e18;
        ZSetMember *m;
        while ((m = sl_next(&it)) != NULL) {
            if (m->score < prev) ordered = 0;
            prev = m->score;
            n++;
        }
        CHECK_EQ_INT(n, N);
        CHECK_MSG(ordered, "large set must iterate in non-decreasing order");

        /* rank iterator over the whole set matches too */
        SkipListIterator rit = sl_iterator_rank(list, 0, N - 1);
        int rn = 0;
        while (sl_next(&rit) != NULL) rn++;
        CHECK_EQ_INT(rn, N);

        sl_free_shallow(list);
    }

    /* All nodes are freed by sl_free_shallow above; free the members we own. */
    free_tracked();

    TF_SUITE_END();
}
