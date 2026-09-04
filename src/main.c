#include "kvstore/common.h"
#include "kvstore/server.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>

volatile sig_atomic_t g_kvc_stop = 0;

static void on_signal(int sig) {
    (void)sig;
    g_kvc_stop = 1;
}

/* Raise RLIMIT_NOFILE so the reactor can hold thousands of concurrent
   connections (the kernel default soft cap is often 256–1024 and the
   Phase 2 acceptance test opens 1,000). Best-effort: only raises, never
   lowers; a failure just means a smaller cap. */
static void raise_fd_limit(void) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) return;
    rl.rlim_cur = rl.rlim_max;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        /* Hard cap may be modest; try 65536 so the soft limit still scales. */
        rl.rlim_cur = 65536;
        (void)setrlimit(RLIMIT_NOFILE, &rl);
    }
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s [-p PORT] [-a ADDR]\n"
            "  -p PORT   listen port (default 6379)\n"
            "  -a ADDR   bind address (default 127.0.0.1; '*' = all)\n"
            "  -h        this help\n",
            prog);
}

int main(int argc, char **argv) {
    int port = 6379;
    const char *addr = "127.0.0.1";

    for (int i = 1; i < argc; i++) {
        if ((strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) &&
            i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if ((strcmp(argv[i], "-a") == 0 || strcmp(argv[i], "--addr") == 0) &&
                   i + 1 < argc) {
            addr = argv[++i];
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return EXIT_SUCCESS;
        } else {
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "invalid port: %d\n", port);
        return EXIT_FAILURE;
    }

    /* Never die on SIGPIPE: a closed peer surfaces as a send() error that
       we handle explicitly (see flush_output in server.c). */
    signal(SIGPIPE, SIG_IGN);

    raise_fd_limit();

    /* Ctrl-C / SIGTERM set the stop flag. We deliberately do NOT use
       SA_RESTART so blocking read()/accept() return EINTR and unwind. */
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) != 0 || sigaction(SIGTERM, &sa, NULL) != 0) {
        kvc_log(KVC_LOG_ERR, "sigaction(): %s", strerror(errno));
        return EXIT_FAILURE;
    }

    kv_server srv;
    if (kv_server_init(&srv, addr, port) != KVC_OK) {
        return EXIT_FAILURE;
    }
    kvc_log(KVC_LOG_INFO, "kvstore ready on %s:%d", addr, port);

    int rc = kv_server_run(&srv);

    kv_server_destroy(&srv);
    kvc_log(KVC_LOG_INFO, "shutdown complete (exit %d)", rc);
    return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}