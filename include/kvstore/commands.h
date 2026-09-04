#ifndef KVC_COMMANDS_H
#define KVC_COMMANDS_H

/*
 * commands.h — command dispatch layer.
 *
 * Takes a fully parsed request (argv/argvlen, as produced by the RESP
 * parser) and appends the RESP reply to *reply. Unknown commands and
 * arity violations are answered with a Redis-style error reply.
 */

#include "common.h"
#include "protocol.h"
#include "store.h"

kvc_err kv_dispatch(kv_store *s, int argc, char **argv,
                    const size_t *argvlen, resp_reply *reply);

#endif /* KVC_COMMANDS_H */