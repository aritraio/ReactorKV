#ifndef KVC_COMMON_H
#define KVC_COMMON_H

/*
 * common.h — shared types, error codes, and strict-allocation helpers.
 *
 * The allocation wrappers never return NULL: they log errno and abort.
 * That keeps error paths manageable while Phase 1 is single-threaded;
 * all OOM handling is centralized here so it can change in one place.
 */

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Error codes                                                         */
/* ------------------------------------------------------------------ */

typedef enum {
    KVC_OK           =  0,
    KVC_ERR_NOMEM    = -1,  /* allocation failure */
    KVC_ERR_NOTFOUND = -2,  /* key absent (or logically expired) */
    KVC_ERR_INVAL    = -3,  /* bad argument / protocol violation */
    KVC_ERR_IO       = -4,  /* syscall-level failure */
} kvc_err;

#define KVC_RET_ERR(expr) \
    do { kvc_err _kvc_err_ = (expr); if (_kvc_err_ != KVC_OK) return _kvc_err_; } while (0)

/* ------------------------------------------------------------------ */
/* Global shutdown flag (defined in main.c)                            */
/* ------------------------------------------------------------------ */

/* A signal handler sets this; loops poll it and unwind cleanly.
   volatile sig_atomic_t is the only type guaranteed safe to touch
   from a signal handler. */
extern volatile sig_atomic_t g_kvc_stop;

/* ------------------------------------------------------------------ */
/* Allocation wrappers — never return NULL; abort with errno on OOM    */
/* ------------------------------------------------------------------ */

void *kvc_malloc(size_t n);
void *kvc_calloc(size_t n, size_t sz);
void *kvc_realloc(void *p, size_t n);
char *kvc_strndup(const char *s, size_t n);

/* ------------------------------------------------------------------ */
/* Utility                                                             */
/* ------------------------------------------------------------------ */

/* Strict decimal parse of exactly n bytes (binary-safe, no trailing junk).
   Accepts an optional leading '-'; rejects '+'. Returns KVC_ERR_INVAL on
   any non-digit or overflow. Used by INCR and EXPIRE. */
kvc_err kvc_parse_int64(const char *s, size_t n, int64_t *out);

/* Milliseconds since the Unix epoch (wall clock; matches EXPIRE semantics). */
int64_t kvc_now_ms(void);

/* ------------------------------------------------------------------ */
/* Logging                                                             */
/* ------------------------------------------------------------------ */

#define KVC_LOG_INFO 0
#define KVC_LOG_WARN 1
#define KVC_LOG_ERR  2

void kvc_log(int level, const char *fmt, ...);

#define KVC_UNUSED(x)    ((void)(x))
#define KVC_ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))
#define KVC_MAX(a, b)    ((a) > (b) ? (a) : (b))

#endif /* KVC_COMMON_H */