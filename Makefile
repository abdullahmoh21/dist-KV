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
              $(SERVER_SRC_DIR)/parser/resp_parser.c \
              $(SERVER_SRC_DIR)/store/buffer.c \
              $(SERVER_SRC_DIR)/store/hashmap.c \
              $(SERVER_SRC_DIR)/store/redis_store.c \
              $(SERVER_SRC_DIR)/store/skip_list.c \
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
.PHONY: release
release: CFLAGS = -Wall -Wextra -std=c11 -O3 -DNDEBUG
release: clean all

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
	@echo "  release      - Build optimized release version"
	@echo "  run-server   - Build and run the server"
	@echo "  run-client   - Build and run the client"
	@echo "  help         - Display this help message"

# Alias targets
.PHONY: server client
server: $(SERVER_BIN)
client: $(CLIENT_BIN)
