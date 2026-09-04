#include "kvstore/commands.h"
#include "kvstore/evloop.h"
#include "kvstore/protocol.h"
#include "kvstore/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/*
 * server.c — Phase 2 reactor.
 *
 * One thread, one event loop. Everything that can block is off the table:
 *   - the listener is non-blocking; accept() drains the backlog on READ
 *     events until EAGAIN;
 *   - each connection is non-blocking with:
 *       rbuf: fixed staging buffer → resp_parser (unchanged Phase 1 seam)
 *       wbuf: outbound reply bytes with a write offset for partial sends
 *            (EAGAIN re-arms WRITE interest, resumed on the next event)
 *   - level-triggered interest is kept in sync by sync_interests(): WRITE
 *     is registered only while there are buffered bytes, otherwise the
 *     kernel would report the fd writable forever (busy spin).
 *
 * Backpressure: if the outbound buffer hits KVC_CONN_WBUF_HIGH, reads are
 * paused (READ interest dropped) so a slow consumer cannot balloon memory;
 * reading resumes once the buffer drains below KVC_CONN_WBUF_LOW.
 *
 * Connection lifetime: each accepted fd gets a kv_conn from the free list,
 * which is recycled on close — memory stays flat under connection churn.
 * Connections with between-read() data (EOF or RST) are closed directly;
 * protocol errors get an error reply queued first (state
 * KVC_CONN_CLOSE_AFTER_WRITE), mirroring Redis.
 */

/* Outbound buffer high/low water marks for read backpressure. */
#define KVC_CONN_WBUF_HIGH (64 * 1024)
#define KVC_CONN_WBUF_LOW  (16 * 1024)

#define KVC_CONN_RBUF_CAP  (16 * 1024)

static void on_fd_event(kv_evloop *el, int fd, uint32_t flags, void *arg);
static void on_stats_timer(kv_evloop *el, void *arg);
static void close_conn(kv_server *srv, kv_conn *c, const char *why);

/* ------------------------------------------------------------------ */
/* Socket setup (unchanged semantics from Phase 1, now non-blocking)    */
/* ------------------------------------------------------------------ */

static int set_nonblocking(int fd, const char *what) {
    int fl = fcntl(fd, F_GETFL);
    if (fl < 0 || fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0) {
        kvc_log(KVC_LOG_ERR, "fcntl(%s) O_NONBLOCK: %s", what, strerror(errno));
        return -1;
    }
    return 0;
}

kvc_err kv_server_init(kv_server *srv, const char *addr, int port) {
    memset(srv, 0, sizeof *srv);
    srv->listen_fd = -1;
    srv->max_conns = KVC_DEFAULT_MAX_CONNS;
    KVC_RET_ERR(kv_store_init(&srv->store, 16));

    /* Phase 4 defaults: unlimited memory, active expiry at 10 Hz. */
    srv->maxmemory = 0;
    srv->expire_interval_ms = KVC_EXPIRE_INTERVAL_MS_DEFAULT;
    srv->expire_sample = KVC_EXPIRE_SAMPLE_DEFAULT;
    atomic_init(&srv->expire.stop, false);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        kvc_log(KVC_LOG_ERR, "socket(): %s", strerror(errno));
        return KVC_ERR_IO;
    }
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) != 0) {
        kvc_log(KVC_LOG_WARN, "setsockopt(SO_REUSEADDR): %s", strerror(errno));
        /* non-fatal: TIME_WAIT reuse is a convenience, not a requirement */
    }
    if (set_nonblocking(fd, "listen") != 0) {
        close(fd);
        return KVC_ERR_IO;
    }

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    if (addr == NULL || strcmp(addr, "*") == 0) {
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, addr, &sa.sin_addr) != 1) {
        kvc_log(KVC_LOG_ERR, "invalid bind address: %s", addr);
        close(fd);
        return KVC_ERR_INVAL;
    }
    if (bind(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        kvc_log(KVC_LOG_ERR, "bind(%s:%d): %s", addr ? addr : "*", port,
                strerror(errno));
        close(fd);
        return KVC_ERR_IO;
    }
    if (listen(fd, SOMAXCONN) != 0) {
        kvc_log(KVC_LOG_ERR, "listen(): %s", strerror(errno));
        close(fd);
        return KVC_ERR_IO;
    }
    srv->listen_fd = fd;
    return KVC_OK;
}

void kv_server_destroy(kv_server *srv) {
    if (srv == NULL) return;
    /* Stop the active expiry worker before touching the store it reads. */
    expire_worker_stop(&srv->expire);
    if (srv->el != NULL) {
        kv_evloop_destroy(srv->el);
        srv->el = NULL;
    }
    /* Tear down every conn ever allocated (fd already closed on close_conn). */
    kv_conn *c = srv->conns_head;
    while (c != NULL) {
        kv_conn *next = c->all_next;
        resp_parser_destroy(&c->parser);
        resp_reply_destroy(&c->reply);
        free(c->rbuf);
        free(c->wbuf);
        free(c);
        c = next;
    }
    srv->conns_head = NULL;
    srv->free_list = NULL;
    if (srv->listen_fd >= 0) {
        close(srv->listen_fd);
        srv->listen_fd = -1;
    }
    kv_store_destroy(&srv->store);
}

/* ------------------------------------------------------------------ */
/* Connection lifecycle                                                */
/* ------------------------------------------------------------------ */

static kv_conn *conn_alloc(kv_server *srv) {
    kv_conn *c = srv->free_list;
    if (c != NULL) {
        srv->free_list = c->next;
    } else {
        c = kvc_calloc(1, sizeof *c);
        resp_parser_init(&c->parser);
        resp_reply_init(&c->reply);
        c->rbuf = kvc_malloc(KVC_CONN_RBUF_CAP);
        c->rcap = KVC_CONN_RBUF_CAP;
        /* wbuf grows on demand up to the backpressure cap; starts small. */
        c->wcap = 4096;
        c->wbuf = kvc_malloc(c->wcap);
        c->fd = -1;
        c->all_next = srv->conns_head;
        srv->conns_head = c;
    }
    return c;
}

static void conn_reset(kv_conn *c) {
    resp_parser_clear(&c->parser); /* full reset: no stale bytes across conns */
    resp_reply_clear(&c->reply);
    c->woff = 0;
    c->wlen = 0;
    c->state = KVC_CONN_OPEN;
    c->eof = false;
    c->read_paused = false;
    c->interest = 0;
}

/* ------------------------------------------------------------------ */
/* Write path                                                          */
/* ------------------------------------------------------------------ */

/* Flush as much of c->wbuf as the socket will take right now (non-blocking:
   stops on EAGAIN). Returns 0 while bytes remain (caller should register
   WRITE interest), 1 when the buffer is fully drained. Always leaves the
   connection valid; never closes it. */
static int flush_output(kv_conn *c) {
    while (c->woff < c->wlen) {
#ifdef MSG_NOSIGNAL
        ssize_t n = send(c->fd, c->wbuf + c->woff, c->wlen - c->woff,
                         MSG_NOSIGNAL);
#else
        ssize_t n = send(c->fd, c->wbuf + c->woff, c->wlen - c->woff, 0);
#endif
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return -1; /* ECONNRESET / EPIPE & co: drop the connection */
        }
        c->woff += (size_t)n;
    }
    /* Compact fully-drained prefix so the buffer doesn't grow unbounded. */
    if (c->woff == c->wlen) {
        c->woff = 0;
        c->wlen = 0;
    } else if (c->woff > 0) {
        memmove(c->wbuf, c->wbuf + c->woff, c->wlen - c->woff);
        c->wlen -= c->woff;
        c->woff = 0;
    }
    return (c->wlen == 0) ? 1 : 0;
}

/* Append reply bytes; grows wbuf as needed (aborts on OOM via kvc_*). */
static void pending_append(kv_conn *c, const char *data, size_t len) {
    if (len > SIZE_MAX - c->wlen) {
        kvc_log(KVC_LOG_ERR, "pending_append overflow");
        exit(EXIT_FAILURE);
    }
    size_t need = c->wlen + len;
    if (need > c->wcap) {
        size_t ncap = c->wcap ? c->wcap : 4096;
        while (ncap < need) ncap *= 2;
        c->wbuf = kvc_realloc(c->wbuf, ncap);
        c->wcap = ncap;
    }
    memcpy(c->wbuf + c->wlen, data, len);
    c->wlen += len;
}

/* Queue the reply and try to flush. Returns the flush result, but the
   connection is only closed by the caller (which uses it to decide
   whether to mark KVC_CONN_CLOSE_AFTER_WRITE). */
static void queue_reply(kv_conn *c, const resp_reply *reply) {
    pending_append(c, reply->buf, reply->len);
    (void)flush_output(c);
}

/* Register exactly what the conn needs: READ unless paused and WRITE iff
   the outbound buffer is non-empty. Unknown/closed conns (fd -1) are
   skipped. Returns false if an update failed and the conn should close. */
static bool sync_interests(kv_server *srv, kv_conn *c) {
    if (c->fd < 0) return true; /* parked or already closed */

    uint32_t want = 0;
    if (!c->read_paused && !c->eof) want |= KVC_EV_READ;
    if (c->woff < c->wlen) want |= KVC_EV_WRITE;
    if (want == c->interest) return true;
    if (kv_evloop_update(srv->el, c->fd, want) != KVC_OK) return false;
    c->interest = want;
    return true;
}

/* ------------------------------------------------------------------ */
/* Read path                                                           */
/* ------------------------------------------------------------------ */

/* Read available bytes, feed the parser, dispatch complete requests, and
   queue replies until EAGAIN, EOF, protocol error, or backpressure pause.
   Returns 1 if the conn lives on, 0 if the conn was closed. */
static int on_readable(kv_server *srv, kv_conn *c, uint32_t flags) {
    if (flags & KVC_EV_ERR && !(flags & KVC_EV_READ)) {
        close_conn(srv, c, "ERR event");
        return 0;
    }

    for (;;) {
        if (c->read_paused) break;
        if (c->wlen - c->woff >= KVC_CONN_WBUF_HIGH) {
            /* Slow consumer: stop reading until the buffer drains. */
            c->read_paused = true;
            (void)sync_interests(srv, c);
            break;
        }
        ssize_t n = read(c->fd, c->rbuf, c->rcap);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_conn(srv, c, "read error");
            return 0;
        }
        if (n == 0) {
            /* Peer FIN. Flush whatever replies are already queued (the
               pipelined-drain loop guarantees pending requests were
               processed), then drop the connection. A client that
               half-closed still gets its replies. */
            if (flush_output(c) < 0) {
                close_conn(srv, c, "flush error on EOF");
                return 0;
            }
            if (c->wlen - c->woff == 0) {
                close_conn(srv, c, "EOF");
                return 0;
            }
            c->eof = true;
            c->state = KVC_CONN_CLOSE_AFTER_WRITE;
            (void)sync_interests(srv, c);
            return 1; /* alive: waiting on a WRITE event to finish */
        }
        int rc = resp_parser_feed(&c->parser, c->rbuf, (size_t)n);
        while (rc == 1) {
            resp_reply_clear(&c->reply);
            kvc_err drc = kv_dispatch(&srv->store, c->parser.argc,
                                      c->parser.argv, c->parser.argvlen,
                                      &c->reply);
            (void)drc; /* dispatch failures are encoded in the reply itself */
            srv->requests_processed++;
            queue_reply(c, &c->reply);
            resp_parser_reset(&c->parser);
            rc = resp_parser_feed(&c->parser, NULL, 0); /* drain pipeline */
        }
        if (rc < 0) {
            /* Protocol error: queue the error reply and drop afterwards. */
            resp_reply_clear(&c->reply);
            resp_reply_error(&c->reply, "ERR Protocol error: malformed request");
            queue_reply(c, &c->reply);
            c->state = KVC_CONN_CLOSE_AFTER_WRITE;
            (void)flush_output(c);
            if (c->wlen - c->woff == 0) {
                close_conn(srv, c, "protocol error (drained)");
                return 0;
            }
            (void)sync_interests(srv, c);
            return 1;
        }
    }
    (void)sync_interests(srv, c);
    return 1;
}

/* ------------------------------------------------------------------ */
/* Close path                                                          */
/* ------------------------------------------------------------------ */

static void close_conn(kv_server *srv, kv_conn *c, const char *why) {
    if (c->fd < 0) return;
    kv_evloop_del(srv->el, c->fd);
    close(c->fd);
    c->fd = -1;
    srv->live_conns--;
    conn_reset(c);
    c->next = srv->free_list; /* recycle for the next accept */
    srv->free_list = c;
    kvc_log(KVC_LOG_INFO, "conn closed: %s", why);
}

/* ------------------------------------------------------------------ */
/* Event loop callbacks                                                */
/* ------------------------------------------------------------------ */

static void on_fd_event(kv_evloop *el, int fd, uint32_t flags, void *arg) {
    KVC_UNUSED(el);
    kv_server *srv = (kv_server *)arg;

    if (fd == srv->listen_fd) {
        /* Listener: accept until EAGAIN. Non-blocking sockets accept
           without MSG_CMSG_CLOEXEC concerns; fds are closed on conn close. */
        for (;;) {
            struct sockaddr_in sa;
            socklen_t slen = sizeof sa;
            int cfd = accept(fd, (struct sockaddr *)&sa, &slen);
            if (cfd < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                kvc_log(KVC_LOG_ERR, "accept(): %s", strerror(errno));
                if (errno == EMFILE || errno == ENFILE) {
                    /* Out of descriptors; back off instead of spinning. */
                    struct timespec ts = { .tv_sec = 0,
                                           .tv_nsec = 50 * 1000 * 1000 };
                    (void)nanosleep(&ts, NULL);
                }
                break;
            }
            if (set_nonblocking(cfd, "conn") != 0) {
                close(cfd);
                continue;
            }
            srv->total_accepted++;

            if (srv->live_conns >= srv->max_conns) {
                /* Politely refuse: reply + close, keep the lock brief. */
                char msg[64];
                int m = snprintf(msg, sizeof msg,
                                 "-ERR max number of clients reached\r\n");
                if (m > 0) (void)send(cfd, msg, (size_t)m, 0);
                close(cfd);
                continue;
            }

            kv_conn *c = conn_alloc(srv);
            conn_reset(c);
            c->fd = cfd;
            c->owner = srv;
            c->interest = KVC_EV_READ;
            srv->live_conns++;
            if (srv->live_conns > srv->peak_conns) {
                srv->peak_conns = srv->live_conns;
            }
            if (kv_evloop_add(srv->el, cfd, KVC_EV_READ, on_fd_event, srv) !=
                KVC_OK) {
                close_conn(srv, c, "evloop add failed");
            }
        }
        return;
    }

    kv_conn *c = NULL;
    for (kv_conn *it = srv->conns_head; it != NULL; it = it->all_next) {
        if (it->fd == fd) {
            c = it;
            break;
        }
    }
    if (c == NULL) return; /* stale event for a closed fd */

    if (flags & KVC_EV_WRITE) {
        int r = flush_output(c);
        if (r < 0) {
            if (c->state == KVC_CONN_CLOSE_AFTER_WRITE && c->wlen - c->woff > 0) {
                /* Try once more with the error reply pending; ignore failure. */
                (void)flush_output(c);
            }
            close_conn(srv, c, "send error");
            return;
        }
        if (c->state == KVC_CONN_CLOSE_AFTER_WRITE && c->wlen - c->woff == 0) {
            close_conn(srv, c, "close-after-write (drained)");
            return;
        }
        if (c->read_paused && c->wlen - c->woff <= KVC_CONN_WBUF_LOW) {
            c->read_paused = false; /* consumer caught up: resume reading */
        }
    }
    if (flags & KVC_EV_READ) {
        /* A conn that is closing (error reply / EOF pending) must not keep
           consuming input — it only waits for WRITE to drain and close. */
        if (c->state != KVC_CONN_CLOSE_AFTER_WRITE) {
            if (on_readable(srv, c, flags) == 0) return; /* closed */
            if (c->fd < 0) return;                        /* closed mid-callback */
        }
    }
    if (flags & KVC_EV_ERR && !(flags & KVC_EV_READ) && c->fd >= 0) {
        close_conn(srv, c, "ERR event");
    }
    if (c->fd >= 0) (void)sync_interests(srv, c);
}

/* Periodic stats: key count from the store (cheap) + conn/request counters. */
static void on_stats_timer(kv_evloop *el, void *arg) {
    kv_server *srv = (kv_server *)arg;
    kv_stats st;
    kv_store_stats(&srv->store, &st);
    kvc_log(KVC_LOG_INFO,
            "stats: keys=%zu hits=%" PRIu64 " misses=%" PRIu64
            " evictions=%" PRIu64 " used=%zu maxmemory=%zu"
            " conns=%d peak=%d accepted=%" PRIu64 " requests=%" PRIu64,
            st.keys, st.hits, st.misses, st.evictions, st.used_bytes,
            st.maxmemory, srv->live_conns, srv->peak_conns,
            srv->total_accepted, srv->requests_processed);
    KVC_UNUSED(el);
}

int kv_server_run(kv_server *srv) {
    srv->el = kv_evloop_create();
    if (srv->el == NULL) return 1;
    kvc_log(KVC_LOG_INFO, "event loop: %s (Phase 2 reactor)",
            kv_evloop_backend_name());

    if (kv_evloop_add(srv->el, srv->listen_fd, KVC_EV_READ, on_fd_event, srv) !=
        KVC_OK) {
        return 1;
    }
    if (kv_evloop_add_timer(srv->el, KVC_STATS_INTERVAL_MS, on_stats_timer,
                            srv) != KVC_OK) {
        return 1;
    }

    /* Phase 4: apply the memory budget and start the active expiry
       worker. kv_server_destroy() stops it (before the store dies). */
    kv_store_set_maxmemory(&srv->store, srv->maxmemory);
    if (expire_worker_start(&srv->expire, &srv->store,
                            srv->expire_interval_ms,
                            srv->expire_sample) != KVC_OK) {
        return 1;
    }

    int rc = kv_evloop_run(srv->el);
    kvc_log(KVC_LOG_INFO,
            "shutdown: peak conns %d, accepted %" PRIu64 ", processed %" PRIu64,
            srv->peak_conns, srv->total_accepted, srv->requests_processed);
    return rc;
}