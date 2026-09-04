#include "kvstore/expire.h"

#include <time.h>

/* Scale the per-pass sample budget so a full keyspace sweep takes ~1s
   (10 passes at the default 100 ms cadence) while bounding CPU for very
   large tables. Reads the lock-free key count between passes. */
#define KVC_EXPIRE_SWEEP_CAP 4096u

static size_t sweep_budget(const expire_worker *w) {
    kv_stats st;
    kv_store_stats(w->store, &st);
    size_t scaled = st.keys / 10;
    size_t budget = w->sample_limit;
    if (scaled > budget) budget = scaled;
    if (budget > KVC_EXPIRE_SWEEP_CAP) budget = KVC_EXPIRE_SWEEP_CAP;
    return budget;
}

static void *expire_worker_main(void *arg) {
    expire_worker *w = (expire_worker *)arg;
    struct timespec delay;
    delay.tv_sec = w->interval_ms / 1000;
    delay.tv_nsec = (long)(w->interval_ms % 1000) * 1000000L;

    while (!atomic_load_explicit(&w->stop, memory_order_relaxed)) {
        struct timespec rem;
        if (nanosleep(&delay, &rem) != 0) {
            if (errno == EINTR) continue;
            break; /* clock/other error: stop trying */
        }
        if (atomic_load_explicit(&w->stop, memory_order_relaxed)) break;
        kv_store_expire_cycle(w->store, sweep_budget(w));
    }
    return NULL;
}

kvc_err expire_worker_start(expire_worker *w, kv_store *store,
                            long interval_ms, size_t sample_limit) {
    memset(w, 0, sizeof *w);
    atomic_init(&w->stop, false);
    if (interval_ms <= 0) return KVC_OK; /* disabled */
    w->store = store;
    w->interval_ms = interval_ms;
    w->sample_limit = sample_limit ? sample_limit : KVC_EXPIRE_SAMPLE_DEFAULT;
    if (pthread_create(&w->thread, NULL, expire_worker_main, w) != 0) {
        return KVC_ERR_IO;
    }
    w->started = true;
    return KVC_OK;
}

void expire_worker_stop(expire_worker *w) {
    if (w == NULL || !w->started) return;
    atomic_store_explicit(&w->stop, true, memory_order_relaxed);
    (void)pthread_join(w->thread, NULL);
    w->started = false;
}
