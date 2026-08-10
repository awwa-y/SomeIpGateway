# ============================================================================
# SomeIpGateway Makefile
# 目标：build/someip_gateway + build/bench_client
# ============================================================================
CXX      ?= g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -Wno-unused-parameter
LDFLAGS  := -lpthread

SRC_DIR   := src
INC_DIR   := include
BUILD_DIR := build

SRCS      := $(SRC_DIR)/main.cpp $(SRC_DIR)/PersistQueue.cpp
OBJS      := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRCS))

TARGET    := $(BUILD_DIR)/someip_gateway
BENCH     := $(BUILD_DIR)/bench_client

.PHONY: all clean run test

all: $(TARGET) $(BENCH)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -I$(INC_DIR) -c $< -o $@

$(TARGET): $(OBJS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BENCH): scripts/bench_client.c | $(BUILD_DIR)
	$(CC) -O2 -Wall $< -o $@

clean:
	rm -rf $(BUILD_DIR)

run: $(TARGET)
	$(TARGET) --port 9090 --hb 5000

test:
	@echo "(TODO) g++ tests/*.cpp -o build/tests && ./build/tests"
