/*
 * Unit tests for the open-chaining FNV-1a hashmap (kv/src/store/hashmap.c).
 * Coverage-driven: exercises insert/get/delete, resize grow/shrink, paused
 * resize, iteration, the hash-cached _h variants, and find-or-insert.
 */
#include "test_framework.h"
#include "store/hashmap.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Test value stored in the map. The map does not copy keys, so the key buffer
 * must outlive every map operation performed on this item. */
typedef struct {
    char key[16];
    int payload;
} TItem;

static KeyView titem_key(void *v) {
    TItem *t = v;
    KeyView k = { t->key, strlen(t->key) };
    return k;
}

int run_hashmap_tests(void) {
    TF_SUITE_BEGIN();

    /* 1. create + single insert + get hit. */
    TEST("create + insert + get hit") {
        HashMap *hm = hm_create(titem_key);
        CHECK_NOT_NULL(hm);

        TItem a = { "alpha", 42 };
        CHECK_EQ_INT(hm_insert(hm, &a), HM_OK);

        void *out = NULL;
        CHECK_EQ_INT(hm_get(hm, a.key, strlen(a.key), &out), HM_OK);
        CHECK(out == &a);
        CHECK_EQ_INT(((TItem *)out)->payload, 42);

        hm_free_shallow(hm);
    }

    /* 2. get miss. */
    TEST("get miss returns HM_NOT_FOUND") {
        HashMap *hm = hm_create(titem_key);
        TItem a = { "present", 1 };
        hm_insert(hm, &a);

        void *out = &a; /* poison: must be untouched-or-null on miss */
        char miss[] = "absent";
        CHECK_EQ_INT(hm_get(hm, miss, strlen(miss), &out), HM_NOT_FOUND);

        hm_free_shallow(hm);
    }

    /* 3. duplicate insert of same key string with different item. */
    TEST("duplicate key insert is rejected") {
        HashMap *hm = hm_create(titem_key);
        TItem a = { "dup", 1 };
        TItem b = { "dup", 2 };
        CHECK_EQ_INT(hm_insert(hm, &a), HM_OK);

        HM_RESULT r = hm_insert(hm, &b);
        CHECK_MSG(r == HM_DUPLICATE, "second insert of same key should be HM_DUPLICATE");

        /* original value must remain in place */
        void *out = NULL;
        CHECK_EQ_INT(hm_get(hm, a.key, strlen(a.key), &out), HM_OK);
        CHECK(out == &a);

        hm_free_shallow(hm);
    }

    /* 4. delete hit + delete miss. */
    TEST("delete hit then miss") {
        HashMap *hm = hm_create(titem_key);
        TItem a = { "gone", 7 };
        hm_insert(hm, &a);

        void *out = NULL;
        CHECK_EQ_INT(hm_delete(hm, a.key, strlen(a.key), &out), HM_OK);
        CHECK(out == &a);

        void *out2 = NULL;
        CHECK_EQ_INT(hm_get(hm, a.key, strlen(a.key), &out2), HM_NOT_FOUND);

        void *out3 = NULL;
        CHECK_EQ_INT(hm_delete(hm, a.key, strlen(a.key), &out3), HM_NOT_FOUND);

        hm_free_shallow(hm);
    }

    /* 5. resize growth: 200 distinct keys forces at least one grow rehash. */
    TEST("resize growth keeps all keys retrievable") {
        HashMap *hm = hm_create(titem_key);
        enum { N = 200 };
        TItem *items = malloc(N * sizeof(TItem));
        CHECK_NOT_NULL(items);

        for (int i = 0; i < N; i++) {
            snprintf(items[i].key, sizeof(items[i].key), "k%d", i);
            items[i].payload = i;
            CHECK_EQ_INT(hm_insert(hm, &items[i]), HM_OK);
        }
        CHECK(hm->size > DEFAULT_SIZE); /* grew */

        int ok = 1;
        for (int i = 0; i < N; i++) {
            void *out = NULL;
            if (hm_get(hm, items[i].key, strlen(items[i].key), &out) != HM_OK ||
                out != &items[i]) {
                ok = 0;
            }
        }
        CHECK_MSG(ok, "all 200 keys retrievable after growth");

        hm_free_shallow(hm);
        free(items);
    }

    /* 6. resize shrink: delete most, survivors still retrievable. */
    TEST("resize shrink keeps survivors retrievable") {
        HashMap *hm = hm_create(titem_key);
        enum { N = 200 };
        TItem *items = malloc(N * sizeof(TItem));
        CHECK_NOT_NULL(items);

        for (int i = 0; i < N; i++) {
            snprintf(items[i].key, sizeof(items[i].key), "s%d", i);
            items[i].payload = i;
            hm_insert(hm, &items[i]);
        }
        size_t grown = hm->size;

        /* delete all but every 40th item (survivors: 0,40,80,120,160) */
        for (int i = 0; i < N; i++) {
            if (i % 40 == 0) continue;
            void *out = NULL;
            hm_delete(hm, items[i].key, strlen(items[i].key), &out);
        }
        CHECK(hm->size < grown); /* shrank */

        int ok = 1;
        for (int i = 0; i < N; i += 40) {
            void *out = NULL;
            if (hm_get(hm, items[i].key, strlen(items[i].key), &out) != HM_OK ||
                out != &items[i]) {
                ok = 0;
            }
        }
        CHECK_MSG(ok, "survivors retrievable after shrink");

        hm_free_shallow(hm);
        free(items);
    }

    /* 7. paused resize: big batch inserted with resize frozen, then resumed. */
    TEST("paused resize keeps items retrievable") {
        HashMap *hm = hm_create(titem_key);
        enum { N = 120 };
        TItem *items = malloc(N * sizeof(TItem));
        CHECK_NOT_NULL(items);

        hm_pause_resize(hm);
        size_t before = hm->size;
        for (int i = 0; i < N; i++) {
            snprintf(items[i].key, sizeof(items[i].key), "p%d", i);
            items[i].payload = i;
            hm_insert(hm, &items[i]);
        }
        CHECK_EQ_INT(hm->size, before); /* did not resize while paused */
        hm_resume_resize(hm);

        int ok = 1;
        for (int i = 0; i < N; i++) {
            void *out = NULL;
            if (hm_get(hm, items[i].key, strlen(items[i].key), &out) != HM_OK ||
                out != &items[i]) {
                ok = 0;
            }
        }
        CHECK_MSG(ok, "all items retrievable across pause/resume");

        hm_free_shallow(hm);
        free(items);
    }

    /* 8. iterator visits every inserted element exactly once. */
    TEST("iterator visits all inserted keys") {
        HashMap *hm = hm_create(titem_key);
        enum { N = 50 };
        TItem items[N];
        char seen[N];
        memset(seen, 0, sizeof(seen));

        for (int i = 0; i < N; i++) {
            snprintf(items[i].key, sizeof(items[i].key), "it%d", i);
            items[i].payload = i;
            hm_insert(hm, &items[i]);
        }

        HMIterator it;
        CHECK_EQ_INT(hm_it_init(hm, &it), HM_OK);

        int count = 0;
        void *out = NULL;
        while (hm_it_next(&it, &out) == HM_OK) {
            TItem *t = out;
            CHECK_NOT_NULL(t);
            /* returned pointer must be one of ours; mark it seen */
            int idx = t->payload;
            if (idx >= 0 && idx < N && &items[idx] == t) {
                seen[idx] = 1;
            }
            count++;
        }
        CHECK_EQ_INT(count, N);

        int all_seen = 1;
        for (int i = 0; i < N; i++) {
            if (!seen[i]) all_seen = 0;
        }
        CHECK_MSG(all_seen, "every inserted key seen exactly once");

        hm_free_shallow(hm);
    }

    /* 9. hash-cached variants agree with the non-_h path. */
    TEST("hash-cached _h variants") {
        HashMap *hm = hm_create(titem_key);

        TItem a = { "cached", 100 };
        size_t h = hm_compute_hash(a.key, strlen(a.key));

        CHECK_EQ_INT(hm_insert_h(hm, &a, h), HM_OK);

        void *out = NULL;
        CHECK_EQ_INT(hm_get_h(hm, a.key, strlen(a.key), h, &out), HM_OK);
        CHECK(out == &a);

        /* same hash also works through the non-_h path */
        void *out2 = NULL;
        CHECK_EQ_INT(hm_get(hm, a.key, strlen(a.key), &out2), HM_OK);
        CHECK(out2 == &a);

        /* find_or_insert_h on an already-present key returns the existing value */
        TItem dup = { "cached", 999 };
        void *existing = NULL;
        CHECK_EQ_INT(hm_find_or_insert_h(hm, &dup, h, &existing), HM_OK);
        CHECK(existing == &a);

        /* find_or_insert_h on a fresh key inserts and reports no existing */
        TItem b = { "cached2", 200 };
        size_t hb = hm_compute_hash(b.key, strlen(b.key));
        void *existing2 = &a; /* poison */
        CHECK_EQ_INT(hm_find_or_insert_h(hm, &b, hb, &existing2), HM_OK);
        CHECK_NULL(existing2);

        /* delete via the cached path */
        void *dout = NULL;
        CHECK_EQ_INT(hm_delete_h(hm, a.key, strlen(a.key), h, &dout), HM_OK);
        CHECK(dout == &a);
        void *gone = NULL;
        CHECK_EQ_INT(hm_get_h(hm, a.key, strlen(a.key), h, &gone), HM_NOT_FOUND);

        hm_free_shallow(hm);
    }

    /* 10. find_or_insert: insert then present. */
    TEST("find_or_insert insert then present") {
        HashMap *hm = hm_create(titem_key);

        TItem a = { "foi", 5 };
        void *existing = &a; /* poison: must be NULL after fresh insert */
        CHECK_EQ_INT(hm_find_or_insert(hm, &a, &existing), HM_OK);
        CHECK_NULL(existing);

        TItem a2 = { "foi", 6 };
        void *existing2 = NULL;
        CHECK_EQ_INT(hm_find_or_insert(hm, &a2, &existing2), HM_OK);
        CHECK(existing2 == &a); /* first value returned, not the new one */

        /* map still holds the original */
        void *out = NULL;
        CHECK_EQ_INT(hm_get(hm, a.key, strlen(a.key), &out), HM_OK);
        CHECK(out == &a);

        hm_free_shallow(hm);
    }

    /* 11. empty-map edge cases. */
    TEST("empty map iterator and delete") {
        HashMap *hm = hm_create(titem_key);

        HMIterator it;
        CHECK_EQ_INT(hm_it_init(hm, &it), HM_OK);
        void *out = NULL;
        CHECK_EQ_INT(hm_it_next(&it, &out), HM_NOT_FOUND);

        char key[] = "nothing";
        void *dout = NULL;
        CHECK_EQ_INT(hm_delete(hm, key, strlen(key), &dout), HM_NOT_FOUND);

        hm_free_shallow(hm);
    }

    TF_SUITE_END();
}
