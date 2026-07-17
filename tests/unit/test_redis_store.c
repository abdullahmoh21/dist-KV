#include "test_framework.h"
#include "store/redis_store.h"
#include "store/object.h"
#include "utils/time.h"
#include <stdint.h>

/* BulkString is the key/value carrier the store API takes by pointer. */
static BulkString bs(const char *s) {
    BulkString b;
    b.data = (char *)s;
    b.len  = strlen(s);
    return b;
}

/* active-expiry callback: bump an int counter per evicted key. */
static void count_cb(const char *key, size_t key_len, void *ctx) {
    (void)key; (void)key_len;
    (*(int *)ctx)++;
}

int run_redis_store_tests(void) {
    TF_SUITE_BEGIN();
    RedisStore store;
    CHECK_EQ_INT(create_store(&store), RS_OK);

    TEST("get on empty store is NOT_FOUND") {
        RedisObject *o = NULL;
        BulkString k = bs("missing");
        CHECK_EQ_INT(rs_get(&store, &k, &o), RS_NOT_FOUND);
    }

    TEST("set then get round-trips value and type") {
        BulkString k = bs("k1"), v = bs("hello");
        CHECK_EQ_INT(rs_set(&store, &k, &v), RS_OK);
        RedisObject *o = NULL;
        CHECK_EQ_INT(rs_get(&store, &k, &o), RS_OK);
        CHECK_NOT_NULL(o);
        CHECK_EQ_INT(o->type, T_KV);
        CHECK_MEM_EQ(o->data, o->data_len, "hello");
    }

    TEST("overwrite updates value and clears any TTL") {
        BulkString k = bs("k1"), v = bs("world!!");
        rs_set(&store, &k, &v);
        RedisObject *o = NULL;
        rs_get(&store, &k, &o);
        CHECK_MEM_EQ(o->data, o->data_len, "world!!");
        CHECK_EQ_INT(o->expire_at_ms, 0);
    }

    TEST("delete removes key; second delete is NOT_FOUND") {
        BulkString k = bs("del"), v = bs("x");
        rs_set(&store, &k, &v);
        CHECK_EQ_INT(rs_delete(&store, &k), RS_OK);
        RedisObject *o = NULL;
        CHECK_EQ_INT(rs_get(&store, &k, &o), RS_NOT_FOUND);
        CHECK_EQ_INT(rs_delete(&store, &k), RS_NOT_FOUND);
    }

    TEST("zadd / zscore basic + update") {
        BulkString zk = bs("z1"), m = bs("member1");
        enum RS_RESULT r = rs_zadd(&store, &zk, &m, 3.5);
        CHECK(r == RS_ADDED || r == RS_OK);
        double sc = 0;
        CHECK_EQ_INT(rs_zscore(&store, &zk, &m, &sc), RS_OK);
        CHECK_EQ_DBL(sc, 3.5);
        /* update existing member's score */
        rs_zadd(&store, &zk, &m, 9.0);
        rs_zscore(&store, &zk, &m, &sc);
        CHECK_EQ_DBL(sc, 9.0);
    }

    TEST("zscore missing member / missing key") {
        BulkString zk = bs("z1"), nope = bs("nope"), missing = bs("zmiss");
        double sc = 0;
        CHECK_EQ_INT(rs_zscore(&store, &zk, &nope, &sc), RS_NOT_FOUND);
        CHECK_EQ_INT(rs_zscore(&store, &missing, &nope, &sc), RS_NOT_FOUND);
    }

    TEST("get_zset returns the set; wrong-type guards") {
        BulkString zk = bs("z1"), kvk = bs("k1"), v = bs("v");
        rs_set(&store, &kvk, &v);
        Zset *zs = NULL;
        CHECK_EQ_INT(rs_get_zset(&store, &zk, &zs), RS_OK);
        CHECK_NOT_NULL(zs);
        /* get_zset on a KV key must not succeed */
        Zset *bad = NULL;
        CHECK(rs_get_zset(&store, &kvk, &bad) == RS_WRONG_TYPE);
        /* rs_get on a zset key must not return it as KV */
        RedisObject *o = NULL;
        CHECK(rs_get(&store, &zk, &o) == RS_WRONG_TYPE);
    }

    TEST("zset_remove_member drops a member") {
        BulkString zk = bs("z2"), a = bs("a"), b = bs("b");
        rs_zadd(&store, &zk, &a, 1.0);
        rs_zadd(&store, &zk, &b, 2.0);
        Zset *zs = NULL;
        rs_get_zset(&store, &zk, &zs);
        CHECK_EQ_INT(rs_zset_remove_member(zs, &a), RS_OK);
        double sc = 0;
        CHECK_EQ_INT(rs_zscore(&store, &zk, &a, &sc), RS_NOT_FOUND);
        CHECK_EQ_INT(rs_zscore(&store, &zk, &b, &sc), RS_OK);
    }

    TEST("TTL lifecycle: none -> set -> persist") {
        BulkString k = bs("ttlk"), v = bs("v");
        rs_set(&store, &k, &v);
        CHECK_EQ_INT(rs_key_ttl_ms(&store, &k), -1);           /* no TTL */
        uint64_t deadline = wallclock_ms() + 100000;
        CHECK_EQ_INT(rs_set_expire(&store, &k, deadline), RS_OK);
        long long ttl = rs_key_ttl_ms(&store, &k);
        CHECK(ttl > 90000 && ttl <= 100000);
        int removed = -1;
        CHECK_EQ_INT(rs_persist(&store, &k, &removed), RS_OK);
        CHECK_EQ_INT(removed, 1);
        CHECK_EQ_INT(rs_key_ttl_ms(&store, &k), -1);
        CHECK_EQ_INT(rs_persist(&store, &k, &removed), RS_OK); /* already persistent */
        CHECK_EQ_INT(removed, 0);
    }

    TEST("set_expire / ttl / persist on missing key") {
        BulkString k = bs("ghost");
        CHECK_EQ_INT(rs_set_expire(&store, &k, wallclock_ms() + 1000), RS_NOT_FOUND);
        CHECK_EQ_INT(rs_key_ttl_ms(&store, &k), -2);
        int removed = -1;
        CHECK_EQ_INT(rs_persist(&store, &k, &removed), RS_NOT_FOUND);
    }

    TEST("lazy expiry deletes on access") {
        BulkString k = bs("lazy"), v = bs("v");
        rs_set(&store, &k, &v);
        CHECK_EQ_INT(rs_set_expire(&store, &k, 1), RS_OK);     /* deadline in the past */
        RedisObject *o = NULL;
        CHECK_EQ_INT(rs_get(&store, &k, &o), RS_NOT_FOUND);    /* lazily reaped */
        CHECK_EQ_INT(rs_key_ttl_ms(&store, &k), -2);
    }

    TEST("active expiry cycle reaps past-deadline keys, spares live ones") {
        /* seed 30 expired keys + 1 live key */
        char kbuf[16];
        for (int i = 0; i < 30; i++) {
            snprintf(kbuf, sizeof(kbuf), "exp%d", i);
            BulkString k = bs(kbuf), v = bs("v");
            rs_set(&store, &k, &v);
            rs_set_expire(&store, &k, 1);   /* past */
        }
        BulkString live = bs("livekey"), lv = bs("v");
        rs_set(&store, &live, &lv);
        rs_set_expire(&store, &live, wallclock_ms() + 100000);

        int evicted = 0;
        /* DEFAULT_SIZE=64 buckets, ~20 per cycle: loop enough to sweep all */
        for (int c = 0; c < 8; c++) {
            rs_active_expire_cycle(&store, wallclock_ms(), count_cb, &evicted);
        }
        CHECK(evicted >= 25);   /* the vast majority of the 30 reaped */
        /* live key survives */
        RedisObject *o = NULL;
        CHECK_EQ_INT(rs_get(&store, &live, &o), RS_OK);
    }

    TEST("zadd / zscore wrong-type against a KV key") {
        BulkString kvk = bs("wrongt"), v = bs("v"), m = bs("m");
        rs_set(&store, &kvk, &v);
        CHECK_EQ_INT(rs_zadd(&store, &kvk, &m, 1.0), RS_WRONG_TYPE);
        double sc = 0;
        CHECK_EQ_INT(rs_zscore(&store, &kvk, &m, &sc), RS_WRONG_TYPE);
    }

    TEST("get_zset on a missing key is NOT_FOUND") {
        BulkString missing = bs("noz");
        Zset *zs = NULL;
        CHECK_EQ_INT(rs_get_zset(&store, &missing, &zs), RS_NOT_FOUND);
    }

    TEST("remove a non-existent member") {
        BulkString zk = bs("z3"), a = bs("a"), ghost = bs("ghost");
        rs_zadd(&store, &zk, &a, 1.0);
        Zset *zs = NULL;
        rs_get_zset(&store, &zk, &zs);
        CHECK(rs_zset_remove_member(zs, &ghost) != RS_OK);
    }

    TEST("delete a zset key frees the whole set") {
        BulkString zk = bs("z4"), a = bs("a");
        rs_zadd(&store, &zk, &a, 1.0);
        CHECK_EQ_INT(rs_delete(&store, &zk), RS_OK);
        Zset *zs = NULL;
        CHECK_EQ_INT(rs_get_zset(&store, &zk, &zs), RS_NOT_FOUND);
    }

    TEST("SET over an expired wrong-type key overwrites it as fresh KV") {
        BulkString zk = bs("ow"), m = bs("m");
        rs_zadd(&store, &zk, &m, 1.0);      /* now a ZSET */
        rs_set_expire(&store, &zk, 1);      /* expired */
        BulkString v = bs("nowkv");
        CHECK_EQ_INT(rs_set(&store, &zk, &v), RS_OK);   /* overwrite, not WRONGTYPE */
        RedisObject *o = NULL;
        CHECK_EQ_INT(rs_get(&store, &zk, &o), RS_OK);
        CHECK_EQ_INT(o->type, T_KV);
        CHECK_MEM_EQ(o->data, o->data_len, "nowkv");
    }

    TEST("get_zset lazily expires an expired zset") {
        BulkString zk = bs("ez"), m = bs("m");
        rs_zadd(&store, &zk, &m, 1.0);
        rs_set_expire(&store, &zk, 1);      /* past */
        Zset *zs = NULL;
        CHECK_EQ_INT(rs_get_zset(&store, &zk, &zs), RS_NOT_FOUND);
    }

    TEST("flush empties the store") {
        CHECK_EQ_INT(rs_flush(&store), RS_OK);
        RedisObject *o = NULL;
        BulkString k = bs("k1");
        CHECK_EQ_INT(rs_get(&store, &k, &o), RS_NOT_FOUND);
    }

    TF_SUITE_END();
}
