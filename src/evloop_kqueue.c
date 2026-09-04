#include "kvstore/evloop.h"

#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>

/*
 * evloop_kqueue.c — BSD/macOS backend over kqueue.
 *
 * One kqueue fd, a registration table indexed by fd, and a periodic-timer
 * table driven by EVFILT_TIMER (NOTE_MSECONDS). Timers and fds live in
 * separate filter namespaces, so a timer ident can never collide with an
 * fd ident.
 *
 * Deleting a registration during callback processing is safe: the table
 * slot is a struct indexed by fd, never freed, so a kevent already pulled
 * into this batch resolves to the fd's *current* slot at dispatch time.
 * A slot that was just closed has live == false and is skipped; a slot
 * reused by an accept in the same batch just delivers an extra READ
 * wake-up, which the reactor's read-until-EAGAIN loop tolerates.
 */

typedef struct kv_fd_rec {
    kv_ev_cb cb;
    void    *arg;
    uint32_t flags;   /* registered interest (KVC_EV_READ|KVC_EV_WRITE) */
    bool     live;
    uint32_t pend;    /* coalesced event flags, set during a batch */
    uint32_t batch;   /* loop iteration that pend belongs to */
} kv_fd_rec;

typedef struct kv_timer {
    int64_t     interval_ms;
    kv_timer_cb cb;
    void       *arg;
} kv_timer;

struct kv_evloop {
    int           kq;
    kv_fd_rec    *by_fd;     /* index by fd */
    size_t        by_fd_cap;
    kv_timer     *timers;    /* idents are indexes into this array */
    size_t        ntimers, timers_cap;
    uint32_t      epoch;     /* increments every kevent batch */
    bool          stop;
};

static bool ensure_cap(kv_fd_rec **arr, size_t *cap, size_t need) {
    if (need < *cap) return true;
    size_t ncap = *cap ? *cap : 64;
    while (ncap <= need) {
        if (ncap > SIZE_MAX / 2) { ncap = need + 1; break; }
        ncap *= 2;
    }
    *arr = kvc_realloc(*arr, ncap * sizeof **arr);
    memset(*arr + *cap, 0, (ncap - *cap) * sizeof **arr);
    *cap = ncap;
    return true;
}

const char *kv_evloop_backend_name(void) { return "kqueue"; }

kv_evloop *kv_evloop_create(void) {
    kv_evloop *el = kvc_calloc(1, sizeof *el);
    el->kq = kqueue();
    if (el->kq < 0) {
        kvc_log(KVC_LOG_ERR, "kqueue(): %s", strerror(errno));
        free(el);
        return NULL;
    }
    return el;
}

void kv_evloop_destroy(kv_evloop *el) {
    if (el == NULL) return;
    if (el->kq >= 0) close(el->kq);
    free(el->by_fd);
    free(el->timers);
    free(el);
}

kvc_err kv_evloop_add(kv_evloop *el, int fd, uint32_t flags,
                      kv_ev_cb cb, void *arg) {
    if (fd < 0) return KVC_ERR_INVAL;
    ensure_cap(&el->by_fd, &el->by_fd_cap, (size_t)fd + 1);
    if (el->by_fd[fd].live) return KVC_ERR_INVAL; /* double registration */

    kv_fd_rec *rec = &el->by_fd[fd];
    rec->cb = cb;
    rec->arg = arg;
    rec->flags = flags;
    rec->live = true;

    struct kevent chg[2];
    int nchg = 0;
    if (flags & KVC_EV_READ) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    }
    if (flags & KVC_EV_WRITE) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
    }
    if (nchg > 0 && kevent(el->kq, chg, nchg, NULL, 0, NULL) < 0) {
        kvc_log(KVC_LOG_ERR, "kevent(EV_ADD, fd %d): %s", fd, strerror(errno));
        rec->live = false;
        rec->flags = 0;
        return KVC_ERR_IO;
    }
    return KVC_OK;
}

kvc_err kv_evloop_update(kv_evloop *el, int fd, uint32_t flags) {
    if (fd < 0 || (size_t)fd >= el->by_fd_cap) return KVC_ERR_NOTFOUND;
    kv_fd_rec *rec = &el->by_fd[fd];
    if (!rec->live) return KVC_ERR_NOTFOUND;
    if (rec->flags == flags) return KVC_OK;

    /* Toggle only the filters whose interest changed. */
    struct kevent chg[2];
    int nchg = 0;
    if ((rec->flags & KVC_EV_READ) && !(flags & KVC_EV_READ)) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    } else if (!(rec->flags & KVC_EV_READ) && (flags & KVC_EV_READ)) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    }
    if ((rec->flags & KVC_EV_WRITE) && !(flags & KVC_EV_WRITE)) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    } else if (!(rec->flags & KVC_EV_WRITE) && (flags & KVC_EV_WRITE)) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
    }
    if (nchg > 0 && kevent(el->kq, chg, nchg, NULL, 0, NULL) < 0 &&
        errno != ENOENT) {
        kvc_log(KVC_LOG_WARN, "kevent(update, fd %d): %s", fd, strerror(errno));
    }
    rec->flags = flags;
    return KVC_OK;
}

kvc_err kv_evloop_del(kv_evloop *el, int fd) {
    if (fd < 0 || (size_t)fd >= el->by_fd_cap) return KVC_ERR_NOTFOUND;
    kv_fd_rec *rec = &el->by_fd[fd];
    if (!rec->live) return KVC_ERR_NOTFOUND;

    struct kevent chg[2];
    int nchg = 0;
    if (rec->flags & KVC_EV_READ) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    }
    if (rec->flags & KVC_EV_WRITE) {
        EV_SET(&chg[nchg++], (uintptr_t)fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    }
    if (nchg > 0 && kevent(el->kq, chg, nchg, NULL, 0, NULL) < 0 &&
        errno != ENOENT) {
        kvc_log(KVC_LOG_WARN, "kevent(EV_DELETE, fd %d): %s", fd, strerror(errno));
    }
    rec->live = false;
    rec->flags = 0;
    rec->cb = NULL;
    rec->arg = NULL;
    return KVC_OK;
}

kvc_err kv_evloop_add_timer(kv_evloop *el, int64_t interval_ms,
                            kv_timer_cb cb, void *arg) {
    if (interval_ms <= 0) return KVC_ERR_INVAL;
    if (el->ntimers == el->timers_cap) {
        size_t ncap = el->timers_cap ? el->timers_cap * 2 : 4;
        el->timers = kvc_realloc(el->timers, ncap * sizeof *el->timers);
        el->timers_cap = ncap;
    }
    kv_timer *t = &el->timers[el->ntimers];
    t->interval_ms = interval_ms;
    t->cb = cb;
    t->arg = arg;

    int fflags = 0;
#ifdef NOTE_MSECONDS
    fflags = NOTE_MSECONDS; /* data is in ms on both macOS and FreeBSD */
#endif
    struct kevent chg;
    EV_SET(&chg, (uintptr_t)el->ntimers, EVFILT_TIMER,
           EV_ADD | EV_ENABLE, fflags, (intptr_t)interval_ms, NULL);
    if (kevent(el->kq, &chg, 1, NULL, 0, NULL) < 0) {
        kvc_log(KVC_LOG_ERR, "kevent(EVFILT_TIMER): %s", strerror(errno));
        return KVC_ERR_IO;
    }
    el->ntimers++;
    return KVC_OK;
}

/* Map one kevent to KVC_EV_* bit flags. */
static uint32_t map_flags(const struct kevent *ev) {
    uint32_t f = 0;
    if (ev->filter == EVFILT_READ) {
        f |= KVC_EV_READ; /* FIN arrives as READ + EV_EOF; the reactor
                             reads 0 bytes and handles the half-close */
    } else if (ev->filter == EVFILT_WRITE) {
        f |= KVC_EV_WRITE;
        /* EV_EOF on the write filter means the peer is gone (RST or
           close); a send() will fail — surface it as ERR. */
        if (ev->flags & EV_EOF) f |= KVC_EV_ERR;
    } else {
        return 0;
    }
    if (ev->flags & EV_ERROR) f |= KVC_EV_ERR;
    return f;
}

void kv_evloop_stop(kv_evloop *el) { el->stop = true; }

int kv_evloop_run(kv_evloop *el) {
    struct kevent evs[KVC_EV_BATCH];
    while (!g_kvc_stop && !el->stop) {
        int n = kevent(el->kq, NULL, 0, evs, KVC_EV_BATCH, NULL);
        if (n < 0) {
            if (errno == EINTR) continue;
            kvc_log(KVC_LOG_ERR, "kevent(): %s", strerror(errno));
            return 1;
        }

        /* Coalesce per-fd: READ and WRITE on the same fd arrive as two
           kevents; merge them so each callback runs once per batch with
           the union of flags (prevents double ERR handling on close). */
        uint32_t epoch = ++el->epoch;
        int touched[KVC_EV_BATCH];
        int ntouched = 0;

        for (int i = 0; i < n; i++) {
            const struct kevent *ev = &evs[i];
            if (ev->filter == EVFILT_TIMER) {
                size_t idx = (size_t)ev->ident;
                if (idx < el->ntimers) {
                    kv_timer *t = &el->timers[idx];
                    t->cb(el, t->arg);
                }
                continue;
            }
            int fd = (int)ev->ident;
            if (fd < 0 || (size_t)fd >= el->by_fd_cap) continue;
            kv_fd_rec *rec = &el->by_fd[fd];
            if (!rec->live) continue;
            uint32_t flags = map_flags(ev);
            if (flags == 0) continue;
            if (rec->batch != epoch) {
                rec->batch = epoch;
                rec->pend = 0;
                touched[ntouched++] = fd;
            }
            rec->pend |= flags;
        }

        for (int i = 0; i < ntouched; i++) {
            int fd = touched[i];
            kv_fd_rec *rec = &el->by_fd[fd];
            if (fd >= (int)el->by_fd_cap) continue;
            if (!rec->live) continue; /* closed before its turn */
            uint32_t flags = rec->pend;
            rec->pend = 0;
            if (rec->cb != NULL) rec->cb(el, fd, flags, rec->arg);
        }
    }
    return 0;
}