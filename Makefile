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
            src/server.c $(EVLOOP_BACKEND)
OBJS      = $(SRCS:.c=.o)
TESTS     = tests/test_hashmap tests/test_protocol tests/test_slab \
            tests/test_lru tests/test_store tests/test_expire

# Phase 4 tests touch the store + worker, so they link the whole core (no
# main.c / server.c: no reactor or sockets needed). pthread is required by
# the rwlock + expiry worker.
THREAD_LIBS = -pthread
CORE_SRCS   = src/store.c src/hashmap.c src/slab.c src/lru.c src/expire.c \
              src/util.c

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(THREAD_LIBS)

src/%.o: src/%.c
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $@ $<

tests/test_hashmap: tests/test_hashmap.c src/hashmap.c src/lru.c src/slab.c src/util.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_hashmap.c src/hashmap.c src/lru.c src/slab.c src/util.c

tests/test_protocol: tests/test_protocol.c src/protocol.c src/commands.c $(CORE_SRCS)
	$(CC) $(TEST_CFLAGS) $(INCLUDES) $(THREAD_LIBS) -o $@ tests/test_protocol.c src/protocol.c src/commands.c $(CORE_SRCS)

tests/test_slab: tests/test_slab.c src/slab.c src/util.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_slab.c src/slab.c src/util.c

tests/test_lru: tests/test_lru.c src/lru.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_lru.c src/lru.c

tests/test_store: tests/test_store.c $(CORE_SRCS)
	$(CC) $(TEST_CFLAGS) $(INCLUDES) $(THREAD_LIBS) -o $@ tests/test_store.c $(CORE_SRCS)

tests/test_expire: tests/test_expire.c $(CORE_SRCS)
	$(CC) $(TEST_CFLAGS) $(INCLUDES) $(THREAD_LIBS) -o $@ tests/test_expire.c $(CORE_SRCS)

test: $(TESTS)
	./tests/test_hashmap
	./tests/test_protocol
	./tests/test_slab
	./tests/test_lru
	./tests/test_store
	./tests/test_expire

smoke: $(BIN)
	python3 tests/smoke.py

# Phase 2 acceptance: many concurrent connections, each pipelining requests.
stress: $(BIN)
	python3 tests/smoke.py --stress

# AddressSanitizer + UndefinedBehaviorSanitizer build of the test suite.
sanitize:
	$(MAKE) clean
	$(MAKE) test TEST_CFLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer"

# ThreadSanitizer build of the Phase 4 concurrency tests (expiry worker +
# store rwlock). Requires a TSan-capable toolchain.
tsan:
	$(MAKE) clean
	$(MAKE) tests/test_store tests/test_expire \
	    TEST_CFLAGS="-O1 -g -fsanitize=thread -fno-omit-frame-pointer"
	./tests/test_store
	./tests/test_expire

valgrind: $(TESTS)
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_hashmap
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_protocol
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_slab
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_lru
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_store
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_expire

clean:
	rm -f $(BIN) $(OBJS) $(TESTS)

.PHONY: all test smoke stress sanitize tsan valgrind clean
