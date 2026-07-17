# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -g -O2
LDFLAGS = 
INCLUDES = -Ikv/include

# Detect OS for event loop backend
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Linux)
    EVENT_LOOP_SRC = kv/src/event_loop/event_loop_epoll.c
    CFLAGS += -DPLATFORM_LINUX
endif

ifeq ($(UNAME_S),Darwin)
    EVENT_LOOP_SRC = kv/src/event_loop/event_loop_kqueue.c
    CFLAGS += -DPLATFORM_MACOS
endif

ifeq ($(EVENT_LOOP_SRC),)
    $(error Unsupported OS: $(UNAME_S). Only Linux and macOS are supported.)
endif

# Directories
SERVER_SRC_DIR = kv/src
SERVER_INC_DIR = kv/include
CLIENT_SRC_DIR = client
BUILD_DIR = build
OBJ_DIR = $(BUILD_DIR)/obj

# Output binaries
SERVER_BIN = server_build
CLIENT_BIN = client_build

# Server source files
SERVER_SRCS = $(SERVER_SRC_DIR)/server.c \
              $(SERVER_SRC_DIR)/engine/execution_engine.c \
              $(SERVER_SRC_DIR)/engine/reply.c \
              $(SERVER_SRC_DIR)/engine/handler/exec_kv.c \
              $(SERVER_SRC_DIR)/engine/handler/exec_expire.c \
              $(SERVER_SRC_DIR)/engine/handler/exec_zset.c \
              $(SERVER_SRC_DIR)/engine/handler/exec_misc.c \
              $(SERVER_SRC_DIR)/parser/resp_parser.c \
              $(SERVER_SRC_DIR)/store/buffer.c \
              $(SERVER_SRC_DIR)/store/hashmap.c \
              $(SERVER_SRC_DIR)/store/redis_store.c \
              $(SERVER_SRC_DIR)/store/skip_list.c \
			  $(SERVER_SRC_DIR)/utils/time.c \
			  $(SERVER_SRC_DIR)/utils/fast_format.c \
			  $(SERVER_SRC_DIR)/utils/fast_parse.c \
			  $(SERVER_SRC_DIR)/aof/aof.c \
			  $(SERVER_SRC_DIR)/aof/aof_manager.c \
			  $(SERVER_SRC_DIR)/aof/aof_load.c \
			  $(SERVER_SRC_DIR)/aof/aof_compact.c \
			  $(SERVER_SRC_DIR)/aof/aof_resp_encode.c \
			  $(SERVER_SRC_DIR)/replication/replication.c \
              $(EVENT_LOOP_SRC)

# Client source files
CLIENT_SRCS = $(CLIENT_SRC_DIR)/client.c

# Object files
SERVER_OBJS = $(patsubst $(SERVER_SRC_DIR)/%.c,$(OBJ_DIR)/server/%.o,$(SERVER_SRCS))
CLIENT_OBJS = $(patsubst $(CLIENT_SRC_DIR)/%.c,$(OBJ_DIR)/client/%.o,$(CLIENT_SRCS))

# Header dependencies
SERVER_HEADERS = $(shell find $(SERVER_INC_DIR) -name '*.h')

# Default target
.PHONY: all
all: $(SERVER_BIN)

# Server binary
$(SERVER_BIN): $(SERVER_OBJS)
	@echo "Linking $@..."
	$(CC) $(CFLAGS) $(SERVER_OBJS) -o $@ $(LDFLAGS)
	@echo "Server built successfully!"

# Client binary
$(CLIENT_BIN): $(CLIENT_OBJS)
	@echo "Linking $@..."
	$(CC) $(CFLAGS) $(CLIENT_OBJS) -o $@ $(LDFLAGS)
	@echo "Client built successfully!"

# Compile server object files
$(OBJ_DIR)/server/%.o: $(SERVER_SRC_DIR)/%.c $(SERVER_HEADERS)
	@mkdir -p $(dir $@)
	@echo "Compiling $<..."
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Compile client object files
$(OBJ_DIR)/client/%.o: $(CLIENT_SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	@echo "Compiling $<..."
	$(CC) $(CFLAGS) -c $< -o $@

# Build both server and client
.PHONY: both
both: $(SERVER_BIN) $(CLIENT_BIN)

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	rm -f $(SERVER_BIN) $(CLIENT_BIN)
	rm -rf $(SERVER_BIN).dSYM $(CLIENT_BIN).dSYM
	@echo "Clean complete!"

# Clean and rebuild
.PHONY: rebuild
rebuild: clean all

# Run the server
.PHONY: run-server
run-server: $(SERVER_BIN)
	./$(SERVER_BIN)

# Run the client
.PHONY: run-client
run-client: $(CLIENT_BIN)
	./$(CLIENT_BIN)

# Debug build (additional debug symbols, no optimization)
.PHONY: debug
debug: CFLAGS = -Wall -Wextra -std=c11 -g -O0 -DDEBUG
debug: clean all

# Release build (optimized, no debug symbols)
# -march=native: enables AVX2/SSE4 for memcpy, memcmp, and the FNV hash loop
# -flto: link-time optimization — inlines hm_get, append_client_output, _sendRaw
#        across translation unit boundaries, where they're called millions of times/sec
.PHONY: release
release: CFLAGS = -Wall -Wextra -std=c11 -O3 -DNDEBUG -march=native -flto
release: clean all

# Profile build (optimized with frame pointers for profilers)
.PHONY: profile
profile: CFLAGS = -O3 -g -fno-omit-frame-pointer
profile: clean all

# Display help information
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all          - Build server (default)"
	@echo "  both         - Build both server and client"
	@echo "  server       - Build server only"
	@echo "  client       - Build client only"
	@echo "  clean        - Remove all build artifacts"
	@echo "  rebuild      - Clean and rebuild"
	@echo "  debug        - Build with debug flags"
	@echo "  profile      - Build with profiling flags"
	@echo "  release      - Build optimized release version"
	@echo "  run-server   - Build and run the server"
	@echo "  run-client   - Build and run the client"
	@echo "  help         - Display this help message"

# Run the isolated AOF compaction test (starts server, bloats AOF, tests compact)
.PHONY: test-compact
test-compact:
	@bash scripts/run_compact_test.sh

# ---------------------------------------------------------------------------
# Test suite + coverage (clang source-based coverage)
# ---------------------------------------------------------------------------
# Two instrumented binaries share one coverage profile:
#   server_cov  - the full server (all sources incl. server.c + event loop),
#                 driven by the Python integration tests over a socket.
#   unit_tests  - every module EXCEPT server.c / the event loop, linked with
#                 the C unit suites; drives the storage/parser/engine layers
#                 in-process (dispatch_command via an injectable reply writer).
# llvm-cov aggregates both binaries against the merged profile, so a line
# covered by EITHER layer counts once.
COV_DIR   = build/cov
COVFLAGS  = -Wall -Wextra -std=c11 -g -O0 -DDEBUG \
            -fprofile-instr-generate -fcoverage-mapping
# Only server.c (owns main + the global `server`) is excluded. The event-loop
# source stays: replication.c link-references event_loop_mod, and exec_misc
# link-references the repl_* symbols. Those files aren't *exercised* by the unit
# suites, but they must resolve at link time; their coverage comes from server_cov.
UNIT_SRCS = $(filter-out $(SERVER_SRC_DIR)/server.c,$(SERVER_SRCS)) \
            $(wildcard tests/unit/*.c)

.PHONY: test-build
test-build: $(COV_DIR)/server_cov $(COV_DIR)/unit_tests

$(COV_DIR)/server_cov: $(SERVER_SRCS) $(SERVER_HEADERS)
	@mkdir -p $(COV_DIR)
	@echo "Building instrumented server_cov..."
	$(CC) $(COVFLAGS) $(INCLUDES) $(SERVER_SRCS) -o $@

$(COV_DIR)/unit_tests: $(UNIT_SRCS) $(SERVER_HEADERS)
	@mkdir -p $(COV_DIR)
	@echo "Building instrumented unit_tests..."
	$(CC) $(COVFLAGS) $(INCLUDES) -Itests/unit $(UNIT_SRCS) -o $@

# Full suite + coverage report; fails if total line coverage < THRESHOLD.
.PHONY: test
test: test-build
	@bash tests/run_tests.sh

# Just the C unit tests, no coverage gate.
.PHONY: test-unit
test-unit: $(COV_DIR)/unit_tests
	@LLVM_PROFILE_FILE=/dev/null $(COV_DIR)/unit_tests

# Alias targets
.PHONY: server client
server: $(SERVER_BIN)
client: $(CLIENT_BIN)
