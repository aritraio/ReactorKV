#include "kvstore/common.h"
#include "kvstore/server.h"

#include <errno.h>
#include <limits.h>
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

/* Parse a byte count with an optional k/m/g (KiB/MiB/GiB) suffix.
   Returns -1 on malformed input or overflow. */
static long long parse_size(const char *s) {
    errno = 0;
    char *end = NULL;
    long long v = strtoll(s, &end, 10);
    if (errno != 0 || end == s) return -1;
    long long mult = 1;
    if (*end != '\0') {
        switch (*end) {
        case 'k': case 'K': mult = 1024LL; break;
        case 'm': case 'M': mult = 1024LL * 1024; break;
        case 'g': case 'G': mult = 1024LL * 1024 * 1024; break;
        default: return -1;
        }
        if (end[1] != '\0') return -1;
    }
    if (v < 0 || v > LLONG_MAX / mult) return -1;
    return v * mult;
}

/* Map a --fsync policy name to the enum; -1 when unknown. */
static int parse_fsync_policy(const char *s) {
    if (strcmp(s, "always") == 0) return WAL_FSYNC_ALWAYS;
    if (strcmp(s, "everysec") == 0) return WAL_FSYNC_EVERYSEC;
    if (strcmp(s, "no") == 0) return WAL_FSYNC_NO;
    return -1;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s [-p PORT] [-a ADDR] [-m BYTES] [-e MS]\n"
            "            [--wal PATH [--fsync POLICY]]\n"
            "  -p PORT       listen port (default 6379)\n"
            "  -a ADDR       bind address (default 127.0.0.1; '*' = all)\n"
            "  -m BYTES      maxmemory budget; evicts LRU tails past it\n"
            "                (0 = unlimited; suffixes k/m/g accepted)\n"
            "  -e MS         active-expiry worker cadence; 0 disables it\n"
            "                (default 100)\n"
            "  --wal PATH    write-ahead log file (default: persistence off;\n"
            "                mutating commands are appended in RESP form and\n"
            "                replayed at startup)\n"
            "  --fsync P     fsync policy when --wal is on: always | everysec\n"
            "                (default) | no\n"
            "  -h            this help\n",
            prog);
}

int main(int argc, char **argv) {
    int port = 6379;
    const char *addr = "127.0.0.1";
    size_t maxmemory = 0;   /* 0 = unlimited */
    long expire_ms = KVC_EXPIRE_INTERVAL_MS_DEFAULT;
    const char *wal_path = NULL;   /* NULL = persistence off */
    int wal_policy = WAL_FSYNC_POLICY_DEFAULT;

    for (int i = 1; i < argc; i++) {
        if ((strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) &&
            i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if ((strcmp(argv[i], "-a") == 0 || strcmp(argv[i], "--addr") == 0) &&
                   i + 1 < argc) {
            addr = argv[++i];
        } else if ((strcmp(argv[i], "-m") == 0 ||
                    strcmp(argv[i], "--maxmemory") == 0) && i + 1 < argc) {
            long long v = parse_size(argv[++i]);
            if (v < 0) {
                fprintf(stderr, "invalid maxmemory: %s\n", argv[i]);
                return EXIT_FAILURE;
            }
            maxmemory = (size_t)v;
        } else if ((strcmp(argv[i], "-e") == 0 ||
                    strcmp(argv[i], "--expire-ms") == 0) && i + 1 < argc) {
            long v = atol(argv[++i]);
            if (v < 0) {
                fprintf(stderr, "invalid expire interval: %s\n", argv[i]);
                return EXIT_FAILURE;
            }
            expire_ms = v;
        } else if (strcmp(argv[i], "--wal") == 0 && i + 1 < argc) {
            wal_path = argv[++i];
        } else if (strcmp(argv[i], "--fsync") == 0 && i + 1 < argc) {
            int p = parse_fsync_policy(argv[++i]);
            if (p < 0) {
                fprintf(stderr, "invalid fsync policy: %s "
                        "(expected always | everysec | no)\n", argv[i]);
                return EXIT_FAILURE;
            }
            wal_policy = p;
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
    srv.maxmemory = maxmemory;
    srv.expire_interval_ms = expire_ms;
    srv.wal_path = wal_path;
    srv.wal_policy = (wal_fsync_policy)wal_policy;
    if (wal_path != NULL) {
        kvc_log(KVC_LOG_INFO,
                "kvstore ready on %s:%d (maxmemory=%zu bytes%s, WAL: %s "
                "fsync=%s)",
                addr, port, maxmemory,
                expire_ms > 0 ? ", expiry worker on" : ", expiry worker off",
                wal_path, wal_policy_name((wal_fsync_policy)wal_policy));
    } else {
        kvc_log(KVC_LOG_INFO,
                "kvstore ready on %s:%d (maxmemory=%zu bytes%s, WAL: off)",
                addr, port, maxmemory,
                expire_ms > 0 ? ", expiry worker on" : ", expiry worker off");
    }

    int rc = kv_server_run(&srv);

    kv_server_destroy(&srv);
    kvc_log(KVC_LOG_INFO, "shutdown complete (exit %d)", rc);
    return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}