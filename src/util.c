#include "kvstore/common.h"

#include <stdarg.h>
#include <time.h>

static const char *level_name(int level) {
    switch (level) {
    case KVC_LOG_INFO: return "info";
    case KVC_LOG_WARN: return "warn";
    default:           return "error";
    }
}

void kvc_log(int level, const char *fmt, ...) {
    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    fprintf(stderr, "[%s] %s\n", level_name(level), msg);
}

void *kvc_malloc(size_t n) {
    if (n == 0) n = 1;
    void *p = malloc(n);
    if (p == NULL) {
        kvc_log(KVC_LOG_ERR, "malloc(%zu) failed: %s", n, strerror(errno));
        exit(EXIT_FAILURE);
    }
    return p;
}

void *kvc_calloc(size_t n, size_t sz) {
    if (n == 0 || sz == 0) { n = 1; sz = 1; }
    if (sz > SIZE_MAX / n) {
        kvc_log(KVC_LOG_ERR, "calloc(%zu, %zu) overflow", n, sz);
        exit(EXIT_FAILURE);
    }
    void *p = calloc(n, sz);
    if (p == NULL) {
        kvc_log(KVC_LOG_ERR, "calloc(%zu, %zu) failed: %s", n, sz, strerror(errno));
        exit(EXIT_FAILURE);
    }
    return p;
}

void *kvc_realloc(void *p, size_t n) {
    if (n == 0) n = 1;
    void *q = realloc(p, n);
    if (q == NULL) {
        kvc_log(KVC_LOG_ERR, "realloc(%zu) failed: %s", n, strerror(errno));
        exit(EXIT_FAILURE);
    }
    return q;
}

char *kvc_strndup(const char *s, size_t n) {
    char *d = kvc_malloc(n + 1);
    memcpy(d, s, n);
    d[n] = '\0';
    return d;
}

kvc_err kvc_parse_int64(const char *s, size_t n, int64_t *out) {
    if (n == 0 || n > 20) return KVC_ERR_INVAL; /* int64 needs at most 20 chars */
    size_t i = 0;
    bool neg = false;
    if (s[0] == '-') {
        neg = true;
        i = 1;
        if (n == 1) return KVC_ERR_INVAL; /* bare '-' */
    }
    uint64_t acc = 0;
    for (; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c < '0' || c > '9') return KVC_ERR_INVAL;
        uint64_t d = (uint64_t)(c - '0');
        if (acc > (UINT64_MAX - d) / 10) return KVC_ERR_INVAL; /* overflow */
        acc = acc * 10 + d;
    }
    if (neg) {
        if (acc > (uint64_t)INT64_MAX + 1) return KVC_ERR_INVAL;
        *out = (acc == (uint64_t)INT64_MAX + 1) ? INT64_MIN : -(int64_t)acc;
    } else {
        if (acc > (uint64_t)INT64_MAX) return KVC_ERR_INVAL;
        *out = (int64_t)acc;
    }
    return KVC_OK;
}

int64_t kvc_now_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        kvc_log(KVC_LOG_ERR, "clock_gettime failed: %s", strerror(errno));
        return (int64_t)time(NULL) * 1000; /* degraded fallback */
    }
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}