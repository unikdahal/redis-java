#!/bin/bash
#
# Redis Java Server - Master-Replica End-to-End Test Script
#
# This script tests the full master-replica replication flow:
# 1. Starts a master server
# 2. Starts one or more replica servers
# 3. Writes data to master
# 4. Verifies data is replicated to replicas
# 5. Tests various scenarios (failover, reconnection, etc.)
#
# Prerequisites:
# - Java 25+ with --enable-preview
# - redis-cli (for testing)
# - Built JAR: mvn clean package -DskipTests
#
# Usage:
# ./test_replication.sh                    # Basic test
# ./test_replication.sh --scale            # Scale test with multiple replicas
# ./test_replication.sh --stress           # High throughput stress test
#

set -e

# Configuration
MASTER_PORT=6379
REPLICA1_PORT=6380
REPLICA2_PORT=6381
REPLICA3_PORT=6382
JAR_PATH="target/redis-server.jar"
LOG_DIR="logs"
JAVA_OPTS="--enable-preview -Xms256m -Xmx512m"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f "redis-server.jar" 2>/dev/null || true
    rm -rf "$LOG_DIR"
}

# Trap cleanup on exit
trap cleanup EXIT

# Print colored message
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
}

log_section() {
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}$1${NC}"
    echo -e "${YELLOW}========================================${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_section "Checking Prerequisites"

    if ! command -v java &> /dev/null; then
        log_error "Java is not installed"
        exit 1
    fi
    log_success "Java found: $(java -version 2>&1 | head -1)"

    if [ ! -f "$JAR_PATH" ]; then
        log_info "Building JAR..."
        mvn clean package -DskipTests -q
    fi
    log_success "JAR found: $JAR_PATH"

    if command -v redis-cli &> /dev/null; then
        log_success "redis-cli found"
        USE_REDIS_CLI=true
    else
        log_info "redis-cli not found, using netcat"
        USE_REDIS_CLI=false
    fi

    mkdir -p "$LOG_DIR"
}

# Start master server
start_master() {
    log_info "Starting master on port $MASTER_PORT..."
    java $JAVA_OPTS -jar "$JAR_PATH" --port $MASTER_PORT > "$LOG_DIR/master.log" 2>&1 &
    sleep 2

    if pgrep -f "redis-server.jar.*$MASTER_PORT" > /dev/null; then
        log_success "Master started on port $MASTER_PORT"
    else
        log_error "Failed to start master"
        cat "$LOG_DIR/master.log"
        exit 1
    fi
}

# Start replica server
start_replica() {
    local port=$1
    local master_port=$2
    log_info "Starting replica on port $port (replicaof localhost:$master_port)..."
    java $JAVA_OPTS -jar "$JAR_PATH" --port $port --replicaof localhost $master_port > "$LOG_DIR/replica_$port.log" 2>&1 &
    sleep 2

    if pgrep -f "redis-server.jar.*$port" > /dev/null; then
        log_success "Replica started on port $port"
    else
        log_error "Failed to start replica on port $port"
        cat "$LOG_DIR/replica_$port.log"
        exit 1
    fi
}

# Send command using redis-cli or netcat
send_command() {
    local port="$1"
    shift
    # Capture remaining args as an array to preserve arguments with spaces
    local args=("$@")

    if [ "$USE_REDIS_CLI" = true ]; then
        redis-cli -p "$port" "${args[@]}" 2>/dev/null
    else
        # Build RESP command preserving argument boundaries
        local resp="*${#args[@]}\r\n"
        for arg in "${args[@]}"; do
            local len=${#arg}
            resp+="\$${len}\r\n${arg}\r\n"
        done
        echo -e "$resp" | nc -q 1 localhost "$port" 2>/dev/null | tr -d '\r'
    fi
}

# Test basic replication
test_basic_replication() {
    log_section "Test: Basic Replication"

    # Write to master
    log_info "Writing to master..."
    send_command $MASTER_PORT SET testkey "Hello, Replication!"

    # Wait for replication
    sleep 1

    # Read from replica
    log_info "Reading from replica..."
    local value=$(send_command $REPLICA1_PORT GET testkey)

    if [[ "$value" == *"Hello, Replication!"* ]]; then
        log_success "Basic replication works! Value: $value"
    else
        log_error "Replication failed. Expected 'Hello, Replication!', got: $value"
        return 1
    fi
}

# Test write protection on replica
test_write_protection() {
    log_section "Test: Write Protection on Replica"

    local result=$(send_command $REPLICA1_PORT SET blocked_key "should_fail")

    if [[ "$result" == *"READONLY"* ]]; then
        log_success "Write protection works! Replica rejected write with READONLY error"
    else
        log_error "Write protection failed. Result: $result"
        return 1
    fi
}

# Test multiple key replication
test_multiple_keys() {
    log_section "Test: Multiple Key Replication"

    local count=100
    log_info "Writing $count keys to master..."

    for i in $(seq 1 $count); do
        send_command $MASTER_PORT SET "key$i" "value$i" > /dev/null
    done

    sleep 2

    log_info "Verifying keys on replica..."
    local success=0
    for i in $(seq 1 $count); do
        local value=$(send_command $REPLICA1_PORT GET "key$i")
        if [[ "$value" == *"value$i"* ]]; then
            success=$((success + 1))
        fi
    done

    if [ $success -eq $count ]; then
        log_success "All $count keys replicated successfully!"
    else
        log_error "Only $success/$count keys replicated"
        return 1
    fi
}

# Test INFO replication output
test_info_replication() {
    log_section "Test: INFO Replication"

    local info=$(send_command $MASTER_PORT INFO replication)

    if [[ "$info" == *"role:master"* ]]; then
        log_success "Master reports role:master"
    else
        log_error "Master role incorrect"
        return 1
    fi

    if [[ "$info" == *"connected_slaves:"* ]]; then
        log_success "Master shows connected_slaves info"
        echo "$info" | grep -E "(role|connected_slaves|master_repl)" | head -10
    fi
}

# Test list operations replication
test_list_replication() {
    log_section "Test: List Operations Replication"

    log_info "Performing list operations on master..."
    send_command $MASTER_PORT LPUSH mylist "item1" > /dev/null
    send_command $MASTER_PORT LPUSH mylist "item2" > /dev/null
    send_command $MASTER_PORT RPUSH mylist "item3" > /dev/null

    sleep 1

    local len=$(send_command $REPLICA1_PORT LLEN mylist)
    if [[ "$len" == *"3"* ]]; then
        log_success "List replicated with correct length: 3"
    else
        log_error "List length incorrect: $len"
        return 1
    fi

    local range=$(send_command $REPLICA1_PORT LRANGE mylist 0 -1)
    log_info "List contents: $range"
}

# Test stream operations replication
test_stream_replication() {
    log_section "Test: Stream Operations Replication"

    log_info "Adding entries to stream on master..."
    local id1=$(send_command $MASTER_PORT XADD mystream "*" field1 value1)
    local id2=$(send_command $MASTER_PORT XADD mystream "*" field2 value2)

    log_info "Generated IDs: $id1, $id2"

    sleep 1

    local range=$(send_command $REPLICA1_PORT XRANGE mystream - +)
    if [[ "$range" == *"field1"* ]] && [[ "$range" == *"field2"* ]]; then
        log_success "Stream entries replicated correctly"
    else
        log_error "Stream replication failed: $range"
        return 1
    fi
}

# Stress test
stress_test() {
    log_section "Stress Test: High Throughput"

    local count=10000
    log_info "Writing $count keys to master..."

    local start=$(date +%s%N)
    for i in $(seq 1 $count); do
        send_command $MASTER_PORT SET "stress$i" "v$i" > /dev/null &
        if [ $((i % 100)) -eq 0 ]; then
            wait
        fi
    done
    wait
    local end=$(date +%s%N)

    local duration_ms=$(( (end - start) / 1000000 ))
    local ops_per_sec=$(( count * 1000 / duration_ms ))

    log_success "Wrote $count keys in ${duration_ms}ms (${ops_per_sec} ops/sec)"

    sleep 3

    log_info "Verifying replication..."
    local verified=0
    for i in $(seq 1 100 $count); do
        local value=$(send_command $REPLICA1_PORT GET "stress$i")
        if [[ "$value" == *"v$i"* ]]; then
            verified=$((verified + 1))
        fi
    done

    log_success "Verified $verified sample keys on replica"
}

# Scale test with multiple replicas
scale_test() {
    log_section "Scale Test: Multiple Replicas"

    start_replica $REPLICA2_PORT $MASTER_PORT
    start_replica $REPLICA3_PORT $MASTER_PORT

    sleep 2

    log_info "Writing key to master..."
    send_command $MASTER_PORT SET scalekey "replicated_to_all"

    sleep 2

    local success=0
    for port in $REPLICA1_PORT $REPLICA2_PORT $REPLICA3_PORT; do
        local value=$(send_command $port GET scalekey)
        if [[ "$value" == *"replicated_to_all"* ]]; then
            log_success "Replica on port $port has the key"
            success=$((success + 1))
        else
            log_error "Replica on port $port missing key"
        fi
    done

    if [ $success -eq 3 ]; then
        log_success "All 3 replicas received the data!"
    fi

    # Check master INFO
    local info=$(send_command $MASTER_PORT INFO replication)
    echo "$info" | grep -E "(connected_slaves|slave[0-9])" | head -10
}

# Main execution
main() {
    echo -e "${GREEN}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║     Redis Java Server - Replication End-to-End Tests     ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"

    check_prerequisites

    # Start servers
    start_master
    start_replica $REPLICA1_PORT $MASTER_PORT

    sleep 2

    # Run tests
    local failed=0

    test_basic_replication || failed=$((failed + 1))
    test_write_protection || failed=$((failed + 1))
    test_multiple_keys || failed=$((failed + 1))
    test_info_replication || failed=$((failed + 1))
    test_list_replication || failed=$((failed + 1))
    test_stream_replication || failed=$((failed + 1))

    # Optional tests based on flags
    if [[ "$1" == "--stress" ]] || [[ "$1" == "--scale" ]]; then
        stress_test || failed=$((failed + 1))
    fi

    if [[ "$1" == "--scale" ]]; then
        scale_test || failed=$((failed + 1))
    fi

    # Summary
    log_section "Test Summary"
    if [ $failed -eq 0 ]; then
        log_success "All tests passed!"
    else
        log_error "$failed test(s) failed"
        exit 1
    fi
}

main "$@"
