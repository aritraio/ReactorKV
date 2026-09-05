/* Phase 5 WAL tests.
 *
 * Drives the store through the real command pipeline (kv_dispatch, which
 * is what appends records in production) against a WAL on a temp file,
 * then reopens + replays into a fresh store — the exact startup path the
 * server uses (records applied with kv_store loading frozen).
 *
 * Covered:
 *   1. Record bytes are canonical RESP; only effective mutations are
 *      logged (read commands are not).
 *   2. EXPIRE is persisted as PEXPIREAT <absolute-ms> and is NOT re-based
 *      by a later restart.
 *   3. Torn-tail records (crash mid-append) are truncated away and the log
 *      stays appendable.
 *   4. Store-driven removals are logged: past-abs PEXPIREAT purge, DEL,
 *      maxmemory evictions, negative-TTL EXPIRE — so replay cannot
 *      resurrect keys or mis-order INCR.
 *   5. Every fsync policy round-trips through a clean close + replay.
 *   6. Mid-file corruption fails closed (server refuses to start).
 */

#include "kvstore/commands.h"
#include "kvstore/store.h"
#include "kvstore/wal.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;
static int checks = 0;
static char wpath[256]; /* temp WAL path (pid-suffixed) */

#define CHECK(cond)                                                        \
    do {                                                                   \
        checks++;                                                          \
        if (!(cond)) {                                                     \
            failures++;                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                  \
    } while (0)

kvc_err replay_store_apply(void *ctx, int argc, char **argv,
                           const size_t *argvlen);

static void msleep(long ms) {
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (ms % 1000) * 1000000L };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {}
}

/* Dispatch one command through the pipeline (argv[0] lowercase, matching
   what the RESP parser produces). */
static void dispatch(kv_store *s, int argc, char **argv) {
    size_t lens[8];
    for (int i = 0; i < argc; i++) lens[i] = strlen(argv[i]);
    resp_reply r;
    resp_reply_init(&r);
    (void)kv_dispatch(s, argc, argv, lens, &r);
    resp_reply_destroy(&r);
}

/* Startup replay: open the WAL at wpath and apply every record into `s`
   with loading frozen, exactly like kv_server_run. Returns the number of
   records replayed, or -1 on failure (the wal is closed in that case). */
static long replay_into(kv_store *s, wal *w, wal_fsync_policy policy) {
    if (wal_open(w, wpath, policy) != KVC_OK) return -1;
    s->loading = true;
    kvc_err rc = wal_replay(w, replay_store_apply, s);
    s->loading = false;
    if (rc != KVC_OK) {
        wal_close(w);
        return -1;
    }
    return (long)w->replayed;
}

/* Replay callback: apply through the real dispatch table; any error reply
   is treated as corruption. */
kvc_err replay_store_apply(void *ctx, int argc, char **argv,
                           const size_t *argvlen) {
    kv_store *s = (kv_store *)ctx;
    resp_reply r;
    resp_reply_init(&r);
    kvc_err rc = kv_dispatch(s, argc, argv, argvlen, &r);
    bool bad = rc != KVC_OK || (r.len > 0 && r.buf[0] == '-');
    if (bad) {
        fprintf(stderr, "FAIL replay rejected record %.*s\n",
                r.len > 0 ? (int)r.len : 0, r.len > 0 ? r.buf : "");
        failures++;
    }
    resp_reply_destroy(&r);
    return bad ? KVC_ERR_IO : KVC_OK;
}

static void check_get(kv_store *s, const char *key, const char *expect) {
    const char *val = NULL;
    size_t val_len = 0;
    kvc_err rc = kv_store_get(s, key, strlen(key), &val, &val_len);
    if (expect == NULL) {
        CHECK(rc == KVC_ERR_NOTFOUND);
        return;
    }
    CHECK(rc == KVC_OK);
    if (rc == KVC_OK) {
        CHECK(val_len == strlen(expect));
        CHECK(memcmp(val, expect, val_len) == 0);
    }
}

/* Peek at an entry's absolute expiry (bypasses expiry checks — fine for
   inspecting a just-replayed store). Returns 0 if the key is missing. */
static int64_t expire_at_of(kv_store *s, const char *key) {
    kv_entry *e = NULL;
    if (hashmap_get(&s->table, key, strlen(key), &e) != KVC_OK) return 0;
    return e->expire_at_ms;
}

/* Read the WAL file bytes (heap; caller frees). */
static char *read_file(const char *path, size_t *len) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return NULL;
    off_t end = lseek(fd, 0, SEEK_END);
    if (end < 0) { close(fd); return NULL; }
    lseek(fd, 0, SEEK_SET);
    char *buf = malloc((size_t)end + 1);
    if (buf == NULL) { close(fd); return NULL; }
    size_t got = 0;
    while (got < (size_t)end) {
        ssize_t n = read(fd, buf + got, (size_t)end - got);
        if (n <= 0) break;
        got += (size_t)n;
    }
    close(fd);
    buf[got] = '\0';
    *len = got;
    return buf;
}

static void raw_append(const char *path, const char *bytes) {
    int fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0644);
    CHECK(fd >= 0);
    if (fd < 0) return;
    size_t left = strlen(bytes);
    const char *p = bytes;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }
        p += n;
        left -= (size_t)n;
    }
    close(fd);
}

/* ------------------------------------------------------------------ */

/* Record bytes are canonical RESP multibulk of the *effective* mutations,
   and read-only commands never touch the log. */
static void test_format_and_readonly_not_logged(void) {
    unlink(wpath);
    kv_store s;
    kv_store_init(&s, 16);
    wal w;
    CHECK(wal_open(&w, wpath, WAL_FSYNC_ALWAYS) == KVC_OK);
    kv_store_set_wal(&s, &w);

    char *seta[] = { (char *)"set", (char *)"a", (char *)"b" };
    char *incr[] = { (char *)"incr", (char *)"n" };
    char *geta[] = { (char *)"get", (char *)"a" };
    char *ping[] = { (char *)"ping" };
    dispatch(&s, 3, seta);
    dispatch(&s, 2, incr);
    dispatch(&s, 2, geta);   /* reads must not be logged */
    dispatch(&s, 1, ping);

    wal_close(&w);
    kv_store_set_wal(&s, NULL);

    size_t flen = 0;
    char *bytes = read_file(wpath, &flen);
    CHECK(bytes != NULL);
    const char *expect = "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\nb\r\n"
                         "*2\r\n$4\r\nincr\r\n$1\r\nn\r\n";
    CHECK(flen == strlen(expect));
    if (flen == strlen(expect)) {
        CHECK(memcmp(bytes, expect, flen) == 0);
    } else {
        fprintf(stderr, "  wal bytes (%zu): [%.*s]\n", flen, (int)flen, bytes);
    }
    free(bytes);

    /* Replay reproduces exactly the mutations. */
    kv_store s2;
    kv_store_init(&s2, 16);
    wal w2;
    long replayed = replay_into(&s2, &w2, WAL_FSYNC_ALWAYS);
    CHECK(replayed == 2);
    kv_store_set_wal(&s2, NULL);
    wal_close(&w2);
    check_get(&s2, "a", "b");
    check_get(&s2, "n", "1");

    kv_store_destroy(&s2);
    kv_store_destroy(&s);
    unlink(wpath);
}

/* EXPIRE is persisted as PEXPIREAT <absolute epoch-ms>; a restart (even
   later) must not re-base the TTL. */
static void test_expire_translated_to_absolute(void) {
    unlink(wpath);
    kv_store s;
    kv_store_init(&s, 16);
    wal w;
    CHECK(wal_open(&w, wpath, WAL_FSYNC_ALWAYS) == KVC_OK);
    kv_store_set_wal(&s, &w);

    char *set[] = { (char *)"set", (char *)"tmp", (char *)"1" };
    dispatch(&s, 3, set);

    int64_t t0 = kvc_now_ms();
    char secs[16];
    snprintf(secs, sizeof secs, "100");
    char *exp[] = { (char *)"expire", (char *)"tmp", secs };
    dispatch(&s, 3, exp);
    int64_t abs_expect = t0 + 100000; /* cmd ran within the same ms */

    /* EXPIRE on a missing key must not be logged. */
    char *exp2[] = { (char *)"expire", (char *)"nokey", secs };
    dispatch(&s, 3, exp2);

    wal_close(&w);
    kv_store_set_wal(&s, NULL);

    /* Wait past the write, then replay: an absolute TTL survives the gap.
       A naive relative re-log would yield ~abs_expect + 60 ms here. */
    msleep(60);
    kv_store s2;
    kv_store_init(&s2, 16);
    wal w2;
    long replayed = replay_into(&s2, &w2, WAL_FSYNC_ALWAYS);
    CHECK(replayed == 2); /* SET + PEXPIREAT; missing-key EXPIRE logged nothing */
    kv_store_set_wal(&s2, NULL);
    wal_close(&w2);

    check_get(&s2, "tmp", "1");
    check_get(&s2, "nokey", NULL);
    int64_t at = expire_at_of(&s2, "tmp");
    CHECK(at >= abs_expect);
    CHECK(at <= abs_expect + 5);
    if (at < abs_expect || at > abs_expect + 5) {
        fprintf(stderr, "  expire_at=%lld abs_expect=%lld\n",
                (long long)at, (long long)abs_expect);
    }

    kv_store_destroy(&s2);
    kv_store_destroy(&s);
    unlink(wpath);
}

/* A record torn by a crash is truncated to the last complete record, and
   the log stays cleanly appendable afterwards. */
static void test_torn_tail_truncation(void) {
    unlink(wpath);
    kv_store s;
    kv_store_init(&s, 16);
    wal w;
    CHECK(wal_open(&w, wpath, WAL_FSYNC_ALWAYS) == KVC_OK);
    kv_store_set_wal(&s, &w);
    char *seta[] = { (char *)"set", (char *)"a", (char *)"1" };
    char *setb[] = { (char *)"set", (char *)"b", (char *)"2" };
    dispatch(&s, 3, seta);
    dispatch(&s, 3, setb);
    wal_close(&w);
    kv_store_set_wal(&s, NULL);
    kv_store_destroy(&s);

    /* Simulate a crash mid-append: a SET record cut inside its value. */
    raw_append(wpath, "*3\r\n$3\r\nset\r\n$1\r\nc\r\n$5\r\nhel");

    kv_store s2;
    kv_store_init(&s2, 16);
    wal w2;
    long replayed = replay_into(&s2, &w2, WAL_FSYNC_ALWAYS);
    CHECK(replayed == 2);
    CHECK(w2.truncated);
    kv_store_set_wal(&s2, &w2); /* server attaches after replay */
    check_get(&s2, "a", "1");
    check_get(&s2, "b", "2");
    check_get(&s2, "c", NULL); /* torn record was cut */

    /* The log continues to append cleanly past the truncation point. */
    char *setc[] = { (char *)"set", (char *)"c", (char *)"3" };
    dispatch(&s2, 3, setc);
    wal_close(&w2);
    kv_store_set_wal(&s2, NULL);
    kv_store_destroy(&s2);

    kv_store s3;
    kv_store_init(&s3, 16);
    wal w3;
    replayed = replay_into(&s3, &w3, WAL_FSYNC_ALWAYS);
    CHECK(replayed == 3); /* a, b, c */
    CHECK(!w3.truncated); /* nothing left to cut on the second boot */
    kv_store_set_wal(&s3, NULL);
    wal_close(&w3);
    check_get(&s3, "a", "1");
    check_get(&s3, "b", "2");
    check_get(&s3, "c", "3");

    kv_store_destroy(&s3);
    unlink(wpath);
}

/* Store-driven removals are durable: past-abs PEXPIREAT (purge), DEL, and
   negative-TTL EXPIRE all land as DEL records, so an INCR that follows an
   expiry cannot be replayed against a resurrected value. */
static void test_removals_are_logged(void) {
    unlink(wpath);
    kv_store s;
    kv_store_init(&s, 16);
    wal w;
    CHECK(wal_open(&w, wpath, WAL_FSYNC_ALWAYS) == KVC_OK);
    kv_store_set_wal(&s, &w);

    char past[24];
    snprintf(past, sizeof past, "%lld", (long long)(kvc_now_ms() - 10000));

    /* k is set to 5, given a TTL already in the past (purged, DEL logged),
       then INCRed: at runtime the purge makes INCR create k = 1. A replay
       that failed to log the purge would compute 5 + 1 = 6 instead. */
    char *setk[] = { (char *)"set", (char *)"k", (char *)"5" };
    char *pastx[] = { (char *)"pexpireat", (char *)"k", past };
    char *incrk[] = { (char *)"incr", (char *)"k" };
    dispatch(&s, 3, setk);
    dispatch(&s, 3, pastx); /* purge + DEL record */
    dispatch(&s, 2, incrk); /* creates k = 1 + INCR record */

    /* Multi-key DEL and negative-TTL EXPIRE deletions are durable too. */
    char *setd1[] = { (char *)"set", (char *)"d1", (char *)"1" };
    char *setd2[] = { (char *)"set", (char *)"d2", (char *)"2" };
    char *sete[] = { (char *)"set", (char *)"e", (char *)"1" };
    dispatch(&s, 3, setd1);
    dispatch(&s, 3, setd2);
    dispatch(&s, 3, sete);
    char *del2[] = { (char *)"del", (char *)"d1", (char *)"d2" };
    dispatch(&s, 3, del2);
    char *exp0[] = { (char *)"expire", (char *)"e", (char *)"0" };
    dispatch(&s, 3, exp0);

    wal_close(&w);
    kv_store_set_wal(&s, NULL);
    kv_store_destroy(&s);

    kv_store s2;
    kv_store_init(&s2, 16);
    wal w2;
    long replayed = replay_into(&s2, &w2, WAL_FSYNC_ALWAYS);
    /* SET k, DEL k, INCR k, SET d1, SET d2, SET e, DEL d1, DEL d2, DEL e */
    CHECK(replayed == 9);
    kv_store_set_wal(&s2, NULL);
    wal_close(&w2);
    check_get(&s2, "k", "1");   /* purge-then-INCR ordering preserved */
    check_get(&s2, "d1", NULL); /* multi-key DEL landed as per-key DELs */
    check_get(&s2, "d2", NULL);
    check_get(&s2, "e", NULL);  /* negative-TTL EXPIRE delete landed */

    kv_store_destroy(&s2);
    unlink(wpath);
}

/* maxmemory evictions are logged as DELs: a crash must not resurrect
   evicted keys after replay. */
static void test_eviction_dels_logged(void) {
    unlink(wpath);
    kv_store s;
    kv_store_init(&s, 16);
    kv_store_set_maxmemory(&s, 640); /* 10 entries of the 64 B class */
    wal w;
    CHECK(wal_open(&w, wpath, WAL_FSYNC_ALWAYS) == KVC_OK);
    kv_store_set_wal(&s, &w);

    for (int i = 0; i < 30; i++) {
        char k[8], v[16];
        snprintf(k, sizeof k, "k%02d", i);
        snprintf(v, sizeof v, "v%08d", i);
        char *set[] = { (char *)"set", k, v };
        dispatch(&s, 3, set);
    }
    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.keys == 10);
    CHECK(st.evictions == 20);

    wal_close(&w);
    kv_store_set_wal(&s, NULL);
    kv_store_destroy(&s);

    /* Replay with no budget: only the logged records apply, so exactly the
       runtime survivors return — no resurrected victims. */
    kv_store s2;
    kv_store_init(&s2, 16);
    wal w2;
    long replayed = replay_into(&s2, &w2, WAL_FSYNC_ALWAYS);
    CHECK(replayed == 50); /* 30 SET + 20 eviction DELs */
    kv_store_set_wal(&s2, NULL);
    wal_close(&w2);
    kv_store_stats(&s2, &st);
    CHECK(st.keys == 10);
    for (int i = 0; i < 30; i++) {
        char k[8], v[16];
        snprintf(k, sizeof k, "k%02d", i);
        snprintf(v, sizeof v, "v%08d", i);
        check_get(&s2, k, i >= 20 ? v : NULL); /* only k20..k29 survive */
    }

    kv_store_destroy(&s2);
    unlink(wpath);
}

/* everysec + no policies round-trip through a clean close (which fsyncs). */
static void test_policies_roundtrip(void) {
    for (int pol = WAL_FSYNC_EVERYSEC; pol <= WAL_FSYNC_NO; pol++) {
        unlink(wpath);
        kv_store s;
        kv_store_init(&s, 16);
        wal w;
        CHECK(wal_open(&w, wpath, (wal_fsync_policy)pol) == KVC_OK);
        kv_store_set_wal(&s, &w);
        char *set[] = { (char *)"set", (char *)"key", (char *)"val" };
        char *incr[] = { (char *)"incr", (char *)"cnt" };
        dispatch(&s, 3, set);
        dispatch(&s, 2, incr);
        wal_close(&w); /* joins the everysec flusher + final fsync */
        kv_store_set_wal(&s, NULL);
        kv_store_destroy(&s);

        kv_store s2;
        kv_store_init(&s2, 16);
        wal w2;
        long replayed = replay_into(&s2, &w2, (wal_fsync_policy)pol);
        CHECK(replayed == 2);
        kv_store_set_wal(&s2, NULL);
        wal_close(&w2);
        check_get(&s2, "key", "val");
        check_get(&s2, "cnt", "1");
        kv_store_destroy(&s2);
        unlink(wpath);
    }
}

/* Mid-file corruption (not a torn tail) fails closed: the server must not
   serve a half-replayed dataset. */
static void test_corruption_fails_closed(void) {
    unlink(wpath);
    kv_store s;
    kv_store_init(&s, 16);
    wal w;
    CHECK(wal_open(&w, wpath, WAL_FSYNC_ALWAYS) == KVC_OK);
    kv_store_set_wal(&s, &w);
    char *seta[] = { (char *)"set", (char *)"a", (char *)"1" };
    char *setb[] = { (char *)"set", (char *)"b", (char *)"2" };
    dispatch(&s, 3, seta);
    dispatch(&s, 3, setb);
    wal_close(&w);
    kv_store_set_wal(&s, NULL);
    kv_store_destroy(&s);

    /* Insert a non-RESP junk line between the two complete records. Record
       1 is exactly 25 bytes: "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n". */
    size_t flen = 0;
    char *bytes = read_file(wpath, &flen);
    CHECK(bytes != NULL);
    CHECK(flen > 25);
    int fd = open(wpath, O_WRONLY | O_TRUNC, 0644);
    CHECK(fd >= 0);
    if (fd >= 0) {
        const char junk[] = "NOTARESP\r\n";
        CHECK(write(fd, bytes, 25) == 25);
        CHECK(write(fd, junk, sizeof junk - 1) == (ssize_t)(sizeof junk - 1));
        CHECK(write(fd, bytes + 25, flen - 25) == (ssize_t)(flen - 25));
        close(fd);
    }
    free(bytes);

    kv_store s2;
    kv_store_init(&s2, 16);
    wal w2;
    CHECK(wal_open(&w2, wpath, WAL_FSYNC_ALWAYS) == KVC_OK);
    s2.loading = true;
    kvc_err rc = wal_replay(&w2, replay_store_apply, &s2);
    s2.loading = false;
    CHECK(rc != KVC_OK); /* corrupt log: refuse to start */
    wal_close(&w2);
    kv_store_destroy(&s2);
    unlink(wpath);
}

int main(void) {
    snprintf(wpath, sizeof wpath, "/tmp/kvc_wal_test_%d.wal", (int)getpid());
    test_format_and_readonly_not_logged();
    test_expire_translated_to_absolute();
    test_torn_tail_truncation();
    test_removals_are_logged();
    test_eviction_dels_logged();
    test_policies_roundtrip();
    test_corruption_fails_closed();
    printf("test_wal: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
