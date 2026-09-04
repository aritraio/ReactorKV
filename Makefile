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

SRCS      = src/main.c src/util.c src/hashmap.c src/protocol.c src/store.c \
            src/commands.c src/server.c $(EVLOOP_BACKEND)
OBJS      = $(SRCS:.c=.o)
TESTS     = tests/test_hashmap tests/test_protocol

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS)

src/%.o: src/%.c
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $@ $<

tests/test_hashmap: tests/test_hashmap.c src/hashmap.c src/util.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_hashmap.c src/hashmap.c src/util.c

tests/test_protocol: tests/test_protocol.c src/protocol.c src/commands.c src/store.c src/hashmap.c src/util.c
	$(CC) $(TEST_CFLAGS) $(INCLUDES) -o $@ tests/test_protocol.c src/protocol.c src/commands.c src/store.c src/hashmap.c src/util.c

test: $(TESTS)
	./tests/test_hashmap
	./tests/test_protocol

smoke: $(BIN)
	python3 tests/smoke.py

# Phase 2 acceptance: many concurrent connections, each pipelining requests.
stress: $(BIN)
	python3 tests/smoke.py --stress

# AddressSanitizer + UndefinedBehaviorSanitizer build of the test suite.
sanitize:
	$(MAKE) clean
	$(MAKE) test TEST_CFLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer"

valgrind: $(TESTS)
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_hashmap
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./tests/test_protocol

clean:
	rm -f $(BIN) $(OBJS) $(TESTS)

.PHONY: all test smoke stress sanitize valgrind clean