#ifndef KVC_SERVER_H
#define KVC_SERVER_H

/*
 * server.h — TCP server. Phase 2: a single-threaded reactor over the
 * evloop — non-blocking listener + connections, level-triggered interest
 * managed by sync_interests() in server.c. kv_server_init sets up the
 * socket and store; kv_server_run starts the event loop; kv_server_destroy
 * tears everything down (loop, connections, store) with zero leaks.
 */

#include "common.h"
#include "evloop.h"
#include "store.h"

/* Cap on simultaneous connections (fd exhaustion mitigation). Because
   conn structs are recycled from a free list, memory stays flat at peak
   concurrency regardless of total connection churn. */
#define KVC_DEFAULT_MAX_CONNS 4096

/* Periodic stats timer interval. */
#define KVC_STATS_INTERVAL_MS 5000

typedef struct kv_server {
    int        listen_fd;
    kv_store   store;
    kv_evloop *el; /* event loop (created in kv_server_run) */

    int      max_conns;
    int      live_conns;
    int      peak_conns;
    kv_conn *conns_head; /* every conn ever allocated (teardown list) */
    kv_conn *free_list;  /* parked conns, recycled on accept */

    uint64_t total_accepted;     /* stats */
    uint64_t requests_processed; /* stats */
} kv_server;

/* Create the listening socket (SO_REUSEADDR, bind, listen, non-blocking).
   addr may be NULL or "*" to bind INADDR_ANY. */
kvc_err kv_server_init(kv_server *srv, const char *addr, int port);
void    kv_server_destroy(kv_server *srv);
/* Starts the event loop and serves until g_kvc_stop is set. Returns 0 on
   a clean signal-driven shutdown, 1 on abnormal termination. */
int     kv_server_run(kv_server *srv);

#endif /* KVC_SERVER_H */