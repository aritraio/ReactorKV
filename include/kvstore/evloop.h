#ifndef KVC_EVLOOP_H
#define KVC_EVLOOP_H

/*
 * evloop.h — single-threaded event loop, Phase 2.
 *
 * One loop multiplexes many non-blocking fds (epoll on Linux, kqueue on
 * BSD/macOS) plus periodic timers (timerfd / EVFILT_TIMER). The loop
 * blocks in epoll_wait()/kevent() until the global g_kvc_stop flag is
 * set, so all waits are real syscall blocks — no busy spin.
 *
 * Rule of the house: fds must be non-blocking and callbacks must stop
 * making progress the moment a syscall returns EAGAIN/EWOULDBLOCK. The
 * reactor then re-arms WRITE interest and picks up where it left off on
 * the next event. Interest is level-triggered on both backends, so a
 * WRITE interest must be dropped once a socket has nothing to send —
 * server.c does this in sync_interests().
 *
 * The parser seam from Phase 1 (resp_parser) plugs in unchanged: each
 * conn owns one, fed from a fixed per-conn read staging buffer.
 */

#include "common.h"
#include "protocol.h"

/* Interest / event flag bits handed to callbacks. */
#define KVC_EV_READ  1u
#define KVC_EV_WRITE 2u
#define KVC_EV_ERR   4u

/* Max events fetched per epoll_wait()/kevent() call. */
#define KVC_EV_BATCH 128

typedef struct kv_evloop kv_evloop;

/* fd callback: flags is any OR of KVC_EV_*. Multiple events on one fd in
   a single batch are coalesced and delivered as one call. */
typedef void (*kv_ev_cb)(kv_evloop *el, int fd, uint32_t flags, void *arg);
/* timer callback: fires periodically while the loop runs. */
typedef void (*kv_timer_cb)(kv_evloop *el, void *arg);

/* ------------------------------------------------------------------ */
/* Connection state — owned by the reactor (server.c)                  */
/* ------------------------------------------------------------------ */

enum {
    KVC_CONN_OPEN = 0,
    KVC_CONN_CLOSE_AFTER_WRITE, /* reply queued (protocol error / EOF);
                                   flush it, then drop the connection  */
};

typedef struct kv_conn {
    int  fd;          /* -1 while parked on the free list */
    int  state;       /* KVC_CONN_* */
    bool eof;         /* peer sent FIN (half-close) */
    bool read_paused; /* reply buffer hit high water: stop reading until
                         it drains below the low-water mark (backpressure) */

    /* read side: fixed staging buffer, fed to resp_parser on every read.
       The parser keeps its own buffer for incomplete requests, so the
       staging buffer never needs to accumulate. */
    char *rbuf;
    size_t rcap;

    resp_parser parser; /* incremental RESP parser (unchanged Phase 1 seam) */
    resp_reply  reply;  /* scratch space for building one reply */

    /* write side: reply bytes with a write offset for partial sends */
    char  *wbuf;
    size_t woff;        /* bytes already delivered to the socket */
    size_t wlen;        /* bytes buffered, waiting on the socket */
    size_t wcap;

    uint32_t interest;  /* flags last registered with the loop */

    struct kv_conn *next;      /* free-list link (reactor-owned) */
    struct kv_conn *all_next;  /* teardown list link (reactor-owned) */
    void           *owner;     /* back-pointer to the kv_server */
} kv_conn;

/* ------------------------------------------------------------------ */
/* Loop API                                                            */
/* ------------------------------------------------------------------ */

const char *kv_evloop_backend_name(void); /* "epoll" or "kqueue" (logging) */

kv_evloop *kv_evloop_create(void);
void       kv_evloop_destroy(kv_evloop *el); /* closes the loop fd; does NOT
                                                close registered fds (the
                                                reactor owns those) */

/* Register fd with interest flags (KVC_EV_READ|KVC_EV_WRITE). cb/arg are
   delivered on each matching event. */
kvc_err kv_evloop_add(kv_evloop *el, int fd, uint32_t flags,
                      kv_ev_cb cb, void *arg);
/* Change the interest set of a registered fd. */
kvc_err kv_evloop_update(kv_evloop *el, int fd, uint32_t flags);
kvc_err kv_evloop_del(kv_evloop *el, int fd);

/* Periodic timer; interval_ms must be > 0. cb fires roughly every
   interval_ms while the loop runs. */
kvc_err kv_evloop_add_timer(kv_evloop *el, int64_t interval_ms,
                            kv_timer_cb cb, void *arg);

/* Run until g_kvc_stop or kv_evloop_stop(). Returns 0 on a clean stop,
   1 on an internal loop failure (e.g. epoll_wait error). */
int  kv_evloop_run(kv_evloop *el);
void kv_evloop_stop(kv_evloop *el);

#endif /* KVC_EVLOOP_H */