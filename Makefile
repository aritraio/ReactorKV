CC      ?= cc
CFLAGS  ?= -O2 -g
CFLAGS  += -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wstrict-prototypes \
           -D_POSIX_C_SOURCE=200809L
TEST_CFLAGS ?= $(CFLAGS)

INCLUDES = -Iinclude
BIN      = kvstore

# Phase 2: platform-split event-loop backend. One of these is compiled and
# linked; the other is intentionally absent from OBJS.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    EVLOOP_BACKEND = src/evloop_epoll.c
else
    EVLOOP_BACKEND = src/evloop_kqueue.c
endif

SRCS      = src/main.c src/util.c src/slab.c src/lru.c src/hashmap.c \
            src/protocol.c src/store.c src/commands.c src/expire.c \
            src/wal.c src/server.c $(EVLOOP_BACKEND)
OBJS      = $(SRCS:.c=.o)
TESTS     = tests/test_hashmap tests/test_protocol tests/test_slab \
            tests/test_lru tests/test_store tests/test_expire tests/test_wal

# Tests that touch the store/commands/wal link the whole core (no main.c /
# server.c: no reactor or sockets needed). pthread is required by the
# rwlock + expiry worker; wal.c needs the RESP writer (protocol.c) and is
# pulled in by both store.c (purge-DEL logging) and commands.c.
THREAD_LIBS = -pthread
CORE_SRCS   = src/store.c src/hashmap.c src/slab.c src/lru.c src/expire.c \
              src/util.c src/wal.c src/protocol.c

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(THREAD_LIBS)

src/%.o: src/%.c
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $@ $<

tests/test_hashmap: tests/test_hashmap.c src/hashmap.c src/lru.c src/slab.c src/util.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_hashmap.c src/hashmap.c src/lru.c src/slab.c src/util.c

tests/test_protocol: tests/test_protocol.c src/commands.c $(CORE_SRCS)
	$(CC) $(TEST_CFLAGS) $(INCLUDES) $(THREAD_LIBS) -o $@ tests/test_protocol.c src/commands.c $(CORE_SRCS)

tests/test_slab: tests/test_slab.c src/slab.c src/util.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_slab.c src/slab.c src/util.c

tests/test_lru: tests/test_lru.c src/lru.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_lru.c src/lru.c

tests/test_store: tests/test_store.c $(CORE_SRCS)
	$(CC) $(TEST_CFLAGS) $(INCLUDES) $(THREAD_LIBS) -o $@ tests/test_store.c $(CORE_SRCS)

tests/test_expire: tests/test_expire.c $(CORE_SRCS)
	$(CC) $(TEST_CFLAGS) $(INCLUDES) $(THREAD_LIBS) -o $@ tests/test_expire.c $(CORE_SRCS)

# Phase 5: WAL unit suite (record format, replay, torn-tail truncation,
# purge-DEL ordering, EXPIRE->PEXPIREAT translation).
tests/test_wal: tests/test_wal.c src/commands.c $(CORE_SRCS)
	$(CC) $(TEST_CFLAGS) $(INCLUDES) $(THREAD_LIBS) -o $@ tests/test_wal.c src/commands.c $(CORE_SRCS)

test: $(TESTS)
	./tests/test_hashmap
	./tests/test_protocol
	./tests/test_slab
	./tests/test_lru
	./tests/test_store
	./tests/test_expire
	./tests/test_wal

smoke: $(BIN)
	python3 tests/smoke.py

# Phase 2 acceptance: many concurrent connections, each pipelining requests.
stress: $(BIN)
	python3 tests/smoke.py --stress

# Phase 5: kill -9 crash / restart durability test over TCP (WAL replay +
# torn-tail recovery are unit-tested in tests/test_wal).
recovery: $(BIN)
	python3 tests/recovery.py

# Phase 5: benchmark harness (starts its own server per run).
#   make bench BENCH_ARGS="--clients 4 --requests 100000 --workload mixed"
bench: $(BIN)
	python3 tests/bench.py $(BENCH_ARGS)

# AddressSanitizer + UndefinedBehaviorSanitizer build of the test suite.
sanitize:
	$(MAKE) clean
	$(MAKE) test TEST_CFLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer"

# ThreadSanitizer build of the concurrency tests (Phase 4 expiry worker +
# store rwlock; Phase 5 WAL append mutex + everysec flusher). Requires a
# TSan-capable toolchain.
tsan:
	$(MAKE) clean
	$(MAKE) tests/test_store tests/test_expire tests/test_wal \
	    TEST_CFLAGS="-O1 -g -fsanitize=thread -fno-omit-frame-pointer"
	./tests/test_store
	./tests/test_expire
	./tests/test_wal

valgrind: $(TESTS)
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_hashmap
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_protocol
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_slab
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_lru
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_store
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_expire
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_wal

clean:
	rm -f $(BIN) $(OBJS) $(TESTS)

.PHONY: all test smoke stress sanitize tsan valgrind clean
