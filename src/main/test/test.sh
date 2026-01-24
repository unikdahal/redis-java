#!/bin/bash

# Configuration
HOST="localhost"
PORT=6379

echo "--- 🧪 Starting Redis Client-Side Tests ---"
echo "Target: $HOST:$PORT"

# Function to send a command and print the result
# Usage: send_test "Name of Test" "RESP_PAYLOAD"
send_test() {
    TEST_NAME=$1
    PAYLOAD=$2

    echo -e "\n[TEST] $TEST_NAME"

    # We use printf to ensure \r\n are sent as real bytes (13, 10)
    # piped into netcat (nc) which sends them to the server
    RESPONSE=$(printf "$PAYLOAD" | nc -w 1 $HOST $PORT)

    # Print the raw bytes we sent (for debugging) and what we got back
    echo "Sent (hex): $(echo -n "$PAYLOAD" | xxd -p)"
    echo "Received:   $RESPONSE"
}

# ------------------------------------------------------------------
# TEST CASES
# ------------------------------------------------------------------

# 1. PING
# Format: *1\r\n$4\r\nPING\r\n
send_test "Basic PING" "*1\r\n\$4\r\nPING\r\n"

# 2. ECHO hello
# Format: *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
send_test "ECHO hello" "*2\r\n\$4\r\nECHO\r\n\$5\r\nhello\r\n"

# 3. SET mykey 100
# Format: *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$3\r\n100\r\n
send_test "SET mykey 100" "*3\r\n\$3\r\nSET\r\n\$5\r\nmykey\r\n\$3\r\n100\r\n"

echo -e "\n--- Tests Completed ---"