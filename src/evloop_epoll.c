#include "kvstore/evloop.h"

#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>

/*
 * evloop_epoll.c — Linux backend over epoll(7) + timerfd(2).
 *
 * Same interface and registration-table semantics as the kqueue backend:
 * a table indexed by fd (never freed, so a stale event already pulled into
 * this batch resolves to the fd's current slot at dispatch time — a just-
 * closed slot is skipped via live == false), and periodic timers backed by
 * one timerfd each with the loop watching it as an ordinary fd.
 *
 * epoll_wait() returns at most one event per fd per call with combined
 * flags, so no coalescing pass is needed; the callback runs directly.
 * EPOLLRDHUP is always requested with EPOLLIN so a remote FIN surfaces as
 * a READ (the reactor reads 0 bytes and handles the half-close).
 */

typedef struct kv_fd_rec {
    kv_ev_cb cb;
    void    *arg;
    uint32_t flags;   /* registered interest */
    bool     live;
} kv_fd_rec;

typedef struct kv_timer {
    int         tfd;
    kv_timer_cb cb;
    void       *arg;
} kv_timer;

struct kv_evloop {
    int           efd;
    kv_fd_rec    *by_fd;   /* index by fd */
    size_t        by_fd_cap;
    kv_timer    **timers;  /* boxed so timerfd recs can point at them */
    size_t        ntimers, timers_cap;
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

static uint32_t to_epoll(uint32_t flags) {
    uint32_t ev = 0;
    if (flags & KVC_EV_READ) ev |= EPOLLIN | EPOLLRDHUP;
    if (flags & KVC_EV_WRITE) ev |= EPOLLOUT;
    return ev;
}

static uint32_t from_epoll(uint32_t ev) {
    uint32_t flags = 0;
    if (ev & (EPOLLIN | EPOLLRDHUP)) flags |= KVC_EV_READ;
    if (ev & EPOLLOUT) flags |= KVC_EV_WRITE;
    if (ev & (EPOLLERR | EPOLLHUP)) flags |= KVC_EV_ERR;
    return flags;
}

const char *kv_evloop_backend_name(void) { return "epoll"; }

kv_evloop *kv_evloop_create(void) {
    kv_evloop *el = kvc_calloc(1, sizeof *el);
    el->efd = epoll_create1(EPOLL_CLOEXEC);
    if (el->efd < 0) {
        kvc_log(KVC_LOG_ERR, "epoll_create1(): %s", strerror(errno));
        free(el);
        return NULL;
    }
    return el;
}

void kv_evloop_destroy(kv_evloop *el) {
    if (el == NULL) return;
    if (el->efd >= 0) close(el->efd);
    for (size_t i = 0; i < el->ntimers; i++) {
        if (el->timers[i]->tfd >= 0) close(el->timers[i]->tfd);
        free(el->timers[i]);
    }
    free(el->timers);
    free(el->by_fd);
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

    struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events = to_epoll(flags);
    ev.data.fd = fd;
    if (epoll_ctl(el->efd, EPOLL_CTL_ADD, fd, &ev) != 0) {
        kvc_log(KVC_LOG_ERR, "epoll_ctl(ADD, fd %d): %s", fd, strerror(errno));
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

    struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events = to_epoll(flags);
    ev.data.fd = fd;
    if (epoll_ctl(el->efd, EPOLL_CTL_MOD, fd, &ev) != 0) {
        kvc_log(KVC_LOG_WARN, "epoll_ctl(MOD, fd %d): %s", fd, strerror(errno));
        return KVC_ERR_IO;
    }
    rec->flags = flags;
    return KVC_OK;
}

kvc_err kv_evloop_del(kv_evloop *el, int fd) {
    if (fd < 0 || (size_t)fd >= el->by_fd_cap) return KVC_ERR_NOTFOUND;
    kv_fd_rec *rec = &el->by_fd[fd];
    if (!rec->live) return KVC_ERR_NOTFOUND;

    if (epoll_ctl(el->efd, EPOLL_CTL_DEL, fd, NULL) != 0 &&
        errno != ENOENT) {
        kvc_log(KVC_LOG_WARN, "epoll_ctl(DEL, fd %d): %s", fd, strerror(errno));
    }
    rec->live = false;
    rec->flags = 0;
    rec->cb = NULL;
    rec->arg = NULL;
    return KVC_OK;
}

/* timerfd became readable: drain it and fire the callback. timerfds with
   an it_interval re-arm themselves, so nothing needs re-registering. */
static void on_timer_fd(kv_evloop *el, int fd, uint32_t flags, void *arg) {
    kv_timer *t = (kv_timer *)arg;
    uint64_t expirations;
    ssize_t n = read(fd, &expirations, sizeof expirations);
    (void)n; /* EAGAIN (missed ticks coalesced) is fine either way */
    KVC_UNUSED(el);
    KVC_UNUSED(flags);
    t->cb(el, t->arg);
}

kvc_err kv_evloop_add_timer(kv_evloop *el, int64_t interval_ms,
                            kv_timer_cb cb, void *arg) {
    if (interval_ms <= 0) return KVC_ERR_INVAL;
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (tfd < 0) {
        kvc_log(KVC_LOG_ERR, "timerfd_create(): %s", strerror(errno));
        return KVC_ERR_IO;
    }
    struct itimerspec its;
    memset(&its, 0, sizeof its);
    its.it_value.tv_sec = interval_ms / 1000;
    its.it_value.tv_nsec = (interval_ms % 1000) * 1000000L;
    its.it_interval = its.it_value;
    if (timerfd_settime(tfd, 0, &its, NULL) != 0) {
        kvc_log(KVC_LOG_ERR, "timerfd_settime(): %s", strerror(errno));
        close(tfd);
        return KVC_ERR_IO;
    }

    kv_timer *t = kvc_calloc(1, sizeof *t);
    t->tfd = tfd;
    t->cb = cb;
    t->arg = arg;

    if (kv_evloop_add(el, tfd, KVC_EV_READ, on_timer_fd, t) != KVC_OK) {
        close(tfd);
        free(t);
        return KVC_ERR_IO;
    }
    if (el->ntimers == el->timers_cap) {
        size_t ncap = el->timers_cap ? el->timers_cap * 2 : 4;
        el->timers = kvc_realloc(el->timers, ncap * sizeof *el->timers);
        el->timers_cap = ncap;
    }
    el->timers[el->ntimers++] = t;
    return KVC_OK;
}

void kv_evloop_stop(kv_evloop *el) { el->stop = true; }

int kv_evloop_run(kv_evloop *el) {
    struct epoll_event evs[KVC_EV_BATCH];
    while (!g_kvc_stop && !el->stop) {
        int n = epoll_wait(el->efd, evs, KVC_EV_BATCH, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            kvc_log(KVC_LOG_ERR, "epoll_wait(): %s", strerror(errno));
            return 1;
        }
        for (int i = 0; i < n; i++) {
            int fd = evs[i].data.fd;
            if (fd < 0) continue;
            kv_fd_rec *rec = (size_t)fd < el->by_fd_cap ? &el->by_fd[fd] : NULL;
            if (rec == NULL || !rec->live) continue;
            uint32_t flags = from_epoll(evs[i].events);
            if (flags == 0) continue;
            if (rec->cb != NULL) rec->cb(el, fd, flags, rec->arg);
        }
    }
    return 0;
}