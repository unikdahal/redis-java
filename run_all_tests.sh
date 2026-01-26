#!/bin/bash
# Test Execution Script for Redis Java Integration Tests
# This script runs all test suites and generates a report

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$PROJECT_DIR"

echo "======================================================================"
echo "  REDIS JAVA - COMPREHENSIVE TEST SUITE EXECUTION"
echo "======================================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# print_section prints a formatted blue section header with the provided title.
print_section() {
    echo ""
    echo -e "${BLUE}===============================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}===============================================${NC}"
    echo ""
}

# run_test executes a test command, increments TOTAL_TESTS and either PASSED_TESTS or FAILED_TESTS, and prints a formatted pass/fail message.
# run_test takes two arguments: the test name (a short label) and the command string to run.
run_test() {
    local test_name=$1
    local test_cmd=$2

    echo -e "${YELLOW}Running: $test_name${NC}"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    if eval "$test_cmd"; then
        echo -e "${GREEN}✅ PASSED: $test_name${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}❌ FAILED: $test_name${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
}

# Build project
print_section "Step 1: Building Project"
echo "Command: mvn clean compile -q"
mvn clean compile -q
echo -e "${GREEN}✅ Build successful${NC}"

# Run Java Unit Tests
print_section "Step 2: Running Java Unit Tests (JUnit5)"
echo "Command: mvn test -q"
run_test "Java Unit Tests" "mvn test -q"

# Build JAR
print_section "Step 3: Building Executable JAR"
echo "Command: mvn package -q -DskipTests"
mvn package -q -DskipTests
JAR_PATH="target/redis-server.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo -e "${RED}❌ JAR not found at $JAR_PATH${NC}"
    exit 1
fi
echo -e "${GREEN}✅ JAR built: $JAR_PATH${NC}"

# Start Redis Server
print_section "Step 4: Starting Redis Server"
PORT=6379
echo "Starting: java --enable-preview -jar $JAR_PATH"
# Start with enable-preview for Java 25 features
java --enable-preview -jar "$JAR_PATH" > /tmp/redis.log 2>&1 &
REDIS_PID=$!
echo "PID: $REDIS_PID"

# Wait for server to accept connections
RETRIES=20
SLEEP_MS=0.25
READY=1
# shellcheck disable=SC2034
for i in $(seq 1 $RETRIES); do
    if nc -z localhost $PORT >/dev/null 2>&1; then
        READY=0
        break
    fi
    sleep $SLEEP_MS
done

if [ $READY -ne 0 ]; then
    echo -e "${RED}❌ Server did not start within expected time. Check /tmp/redis.log${NC}"
    echo "--- /tmp/redis.log ---"
    tail -n +1 /tmp/redis.log || true
    kill $REDIS_PID 2>/dev/null || true
    exit 1
fi

echo -e "${GREEN}✅ Server started and accepting connections on port $PORT${NC}"

# Run Shell Integration Tests (netcat based)
print_section "Step 5: Running Integration Tests (netcat)"
INTEGRATION_SCRIPT="src/main/test/integration_tests.sh"
if [ ! -x "$INTEGRATION_SCRIPT" ]; then
    chmod +x "$INTEGRATION_SCRIPT" || true
fi
run_test "Integration Tests" "bash $INTEGRATION_SCRIPT"

# Stop Redis Server
print_section "Step 6: Cleanup"
echo "Stopping Redis Server (PID: $REDIS_PID)"
kill $REDIS_PID 2>/dev/null || true
sleep 1
if ps -p $REDIS_PID >/dev/null 2>&1; then
    echo "Server did not exit, sending SIGKILL"
    kill -9 $REDIS_PID 2>/dev/null || true
fi

echo -e "${GREEN}✅ Server stopped${NC}"

# Print Summary
print_section "TEST EXECUTION SUMMARY"

echo "Total Tests Run:  $TOTAL_TESTS"
echo -e "Passed Tests:     ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed Tests:     ${RED}$FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo ""
    echo -e "${GREEN}════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  🎉 ALL TESTS PASSED! 🎉${NC}"
    echo -e "${GREEN}════════════════════════════════════════════${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}════════════════════════════════════════════${NC}"
    echo -e "${RED}  ❌ SOME TESTS FAILED ❌${NC}"
    echo -e "${RED}════════════════════════════════════════════${NC}"
    exit 1
fi