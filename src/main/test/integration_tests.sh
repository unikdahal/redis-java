#!/usr/bin/env bash
# Comprehensive integration tests for Redis Java server.
# Uses redis-cli style inline commands via netcat.

set -euo pipefail

HOST=localhost
PORT=6379
NC_TIMEOUT=2

TOTAL=0
PASSED=0
FAILED=0

# Send RESP command and get response
send_cmd() {
    local cmd="$1"
    # Convert command to RESP format
    local args=($cmd)
    local num_args=${#args[@]}
    local resp="*${num_args}\r\n"

    for arg in "${args[@]}"; do
        resp+="\$${#arg}\r\n${arg}\r\n"
    done

    printf '%b' "$resp" | nc -w $NC_TIMEOUT $HOST $PORT 2>/dev/null || echo ""
}

# Run test and check result
run_test() {
    local name="$1"
    local cmd="$2"
    local expected="$3"

    TOTAL=$((TOTAL + 1))

    local result
    result=$(send_cmd "$cmd")

    # Normalize: remove CR/LF for comparison
    result=$(echo -n "$result" | tr -d '\r\n')
    expected_norm=$(echo -n "$expected" | tr -d '\r\n')

    if [[ "$result" == *"$expected_norm"* ]]; then
        PASSED=$((PASSED + 1))
        echo "[PASS] $name"
        return 0
    else
        FAILED=$((FAILED + 1))
        echo "[FAIL] $name"
        echo "  Command:  $cmd"
        echo "  Expected: $expected_norm"
        echo "  Got:      $result"
        return 1
    fi
}

# Run test expecting substring match
run_test_contains() {
    local name="$1"
    local cmd="$2"
    shift 2
    local expected_parts=("$@")

    TOTAL=$((TOTAL + 1))

    local result
    result=$(send_cmd "$cmd")
    result=$(echo -n "$result" | tr -d '\r\n')

    local all_match=1
    for exp in "${expected_parts[@]}"; do
        if [[ "$result" != *"$exp"* ]]; then
            all_match=0
            break
        fi
    done

    if [[ $all_match -eq 1 ]]; then
        PASSED=$((PASSED + 1))
        echo "[PASS] $name"
        return 0
    else
        FAILED=$((FAILED + 1))
        echo "[FAIL] $name"
        echo "  Command:  $cmd"
        echo "  Expected parts: ${expected_parts[*]}"
        echo "  Got: $result"
        return 1
    fi
}

echo "========================================"
echo "  Redis Java Integration Tests"
echo "========================================"
echo ""

# ==================== PING Tests (10) ====================
echo "--- PING Tests ---"
run_test "PING-basic" "PING" "+PONG"
run_test_contains "PING-hello" "PING hello" "hello"
run_test_contains "PING-world" "PING world" "world"
run_test_contains "PING-number" "PING 12345" "12345"
run_test_contains "PING-spaces" "PING test" "test"
run_test_contains "PING-long" "PING longmessagehere" "longmessagehere"
run_test_contains "PING-special" "PING abc123" "abc123"
run_test_contains "PING-single" "PING x" "x"
run_test_contains "PING-mixed" "PING MixedCase" "MixedCase"
run_test_contains "PING-end" "PING end" "end"

# ==================== ECHO Tests (10) ====================
echo ""
echo "--- ECHO Tests ---"
run_test_contains "ECHO-hello" "ECHO hello" "hello"
run_test_contains "ECHO-world" "ECHO world" "world"
run_test_contains "ECHO-number" "ECHO 9876543210" "9876543210"
run_test_contains "ECHO-single" "ECHO a" "a"
run_test_contains "ECHO-mixed" "ECHO AbCdEf" "AbCdEf"
run_test_contains "ECHO-long" "ECHO verylongstringtoecho" "verylongstringtoecho"
run_test_contains "ECHO-special" "ECHO test123" "test123"
run_test_contains "ECHO-caps" "ECHO UPPERCASE" "UPPERCASE"
run_test_contains "ECHO-lower" "ECHO lowercase" "lowercase"
run_test_contains "ECHO-end" "ECHO done" "done"

# ==================== SET/GET Tests (10) ====================
echo ""
echo "--- SET/GET Tests ---"
for i in {1..10}; do
    key="testkey$i"
    val="value$i"
    send_cmd "DEL $key" > /dev/null
    run_test "SET-$i" "SET $key $val" "+OK"
    run_test_contains "GET-$i" "GET $key" "$val"
done

# ==================== DEL Tests (10) ====================
echo ""
echo "--- DEL Tests ---"
for i in {1..5}; do
    key="deltest$i"
    send_cmd "SET $key val$i" > /dev/null
    run_test_contains "DEL-exist-$i" "DEL $key" ":1"
    run_test_contains "DEL-nonexist-$i" "DEL $key" ":0"
done

# ==================== LPUSH Tests (10) ====================
echo ""
echo "--- LPUSH Tests ---"
for i in {1..10}; do
    key="lpushtest$i"
    send_cmd "DEL $key" > /dev/null
    run_test_contains "LPUSH-first-$i" "LPUSH $key elem1" ":1"
    run_test_contains "LPUSH-second-$i" "LPUSH $key elem2" ":2"
    run_test_contains "LPUSH-third-$i" "LPUSH $key elem3" ":3"
done

# ==================== RPUSH Tests (10) ====================
echo ""
echo "--- RPUSH Tests ---"
for i in {1..10}; do
    key="rpushtest$i"
    send_cmd "DEL $key" > /dev/null
    run_test_contains "RPUSH-first-$i" "RPUSH $key item1" ":1"
    run_test_contains "RPUSH-second-$i" "RPUSH $key item2" ":2"
    run_test_contains "RPUSH-third-$i" "RPUSH $key item3" ":3"
done

# ==================== LRANGE Tests (10) ====================
echo ""
echo "--- LRANGE Tests ---"
send_cmd "DEL lrangetest" > /dev/null
send_cmd "RPUSH lrangetest A" > /dev/null
send_cmd "RPUSH lrangetest B" > /dev/null
send_cmd "RPUSH lrangetest C" > /dev/null
send_cmd "RPUSH lrangetest D" > /dev/null
send_cmd "RPUSH lrangetest E" > /dev/null

run_test_contains "LRANGE-full" "LRANGE lrangetest 0 -1" "A" "B" "C" "D" "E"
run_test_contains "LRANGE-partial" "LRANGE lrangetest 1 3" "B" "C" "D"
run_test_contains "LRANGE-negative" "LRANGE lrangetest -3 -1" "C" "D" "E"
run_test_contains "LRANGE-single" "LRANGE lrangetest 0 0" "A"
run_test_contains "LRANGE-last" "LRANGE lrangetest -1 -1" "E"
run_test_contains "LRANGE-outofbound" "LRANGE lrangetest 0 100" "A" "B" "C" "D" "E"
run_test_contains "LRANGE-startbig" "LRANGE lrangetest 10 20" "*0"
run_test_contains "LRANGE-nonexist" "LRANGE nonexistentlist 0 -1" "*0"
run_test_contains "LRANGE-middle" "LRANGE lrangetest 2 2" "C"
run_test_contains "LRANGE-firsttwo" "LRANGE lrangetest 0 1" "A" "B"

# ==================== LLEN Tests (10) ====================
echo ""
echo "--- LLEN Tests ---"
send_cmd "DEL llentest" > /dev/null
run_test_contains "LLEN-nonexist" "LLEN llentest" ":0"
send_cmd "RPUSH llentest x" > /dev/null
run_test_contains "LLEN-1" "LLEN llentest" ":1"
send_cmd "RPUSH llentest y" > /dev/null
run_test_contains "LLEN-2" "LLEN llentest" ":2"
send_cmd "RPUSH llentest z" > /dev/null
run_test_contains "LLEN-3" "LLEN llentest" ":3"

send_cmd "DEL llentest2" > /dev/null
for j in {1..5}; do
    send_cmd "RPUSH llentest2 item$j" > /dev/null
done
run_test_contains "LLEN-5" "LLEN llentest2" ":5"

send_cmd "SET stringkey value" > /dev/null
run_test_contains "LLEN-wrongtype" "LLEN stringkey" "WRONGTYPE"

send_cmd "DEL llentest3" > /dev/null
send_cmd "LPUSH llentest3 a" > /dev/null
send_cmd "LPUSH llentest3 b" > /dev/null
run_test_contains "LLEN-lpush" "LLEN llentest3" ":2"

send_cmd "DEL llentest4" > /dev/null
send_cmd "RPUSH llentest4 x" > /dev/null
send_cmd "LPOP llentest4" > /dev/null
run_test_contains "LLEN-afterpop" "LLEN llentest4" ":0"

# ==================== LPOP Tests (10) ====================
echo ""
echo "--- LPOP Tests ---"
send_cmd "DEL lpoptest" > /dev/null
send_cmd "RPUSH lpoptest A" > /dev/null
send_cmd "RPUSH lpoptest B" > /dev/null
send_cmd "RPUSH lpoptest C" > /dev/null

run_test_contains "LPOP-first" "LPOP lpoptest" "A"
run_test_contains "LPOP-second" "LPOP lpoptest" "B"
run_test_contains "LPOP-third" "LPOP lpoptest" "C"
run_test_contains "LPOP-empty" "LPOP lpoptest" '$-1'

send_cmd "DEL lpoptest2" > /dev/null
send_cmd "RPUSH lpoptest2 1" > /dev/null
send_cmd "RPUSH lpoptest2 2" > /dev/null
send_cmd "RPUSH lpoptest2 3" > /dev/null
send_cmd "RPUSH lpoptest2 4" > /dev/null
send_cmd "RPUSH lpoptest2 5" > /dev/null

run_test_contains "LPOP-count2" "LPOP lpoptest2 2" "*2" "1" "2"
run_test_contains "LPOP-count10" "LPOP lpoptest2 10" "*3" "3" "4" "5"

run_test_contains "LPOP-nonexist" "LPOP nonexistentkey" '$-1'

send_cmd "SET strkey val" > /dev/null
run_test_contains "LPOP-wrongtype" "LPOP strkey" "WRONGTYPE"

send_cmd "DEL lpoptest3" > /dev/null
send_cmd "RPUSH lpoptest3 only" > /dev/null
run_test_contains "LPOP-single" "LPOP lpoptest3" "only"

# ==================== BLPOP Tests (10) ====================
echo ""
echo "--- BLPOP Tests ---"
send_cmd "DEL blpoptest" > /dev/null
send_cmd "RPUSH blpoptest elem1" > /dev/null
run_test_contains "BLPOP-immediate" "BLPOP blpoptest 0" "blpoptest" "elem1"

send_cmd "DEL blpoptest2" > /dev/null
run_test_contains "BLPOP-timeout" "BLPOP blpoptest2 0" "*-1"

send_cmd "DEL bkey1 bkey2" > /dev/null
send_cmd "RPUSH bkey2 val2" > /dev/null
run_test_contains "BLPOP-multikey" "BLPOP bkey1 bkey2 0" "bkey2" "val2"

send_cmd "DEL bltest" > /dev/null
send_cmd "RPUSH bltest a" > /dev/null
send_cmd "RPUSH bltest b" > /dev/null
run_test_contains "BLPOP-seq1" "BLPOP bltest 0" "bltest" "a"
run_test_contains "BLPOP-seq2" "BLPOP bltest 0" "bltest" "b"
run_test_contains "BLPOP-seq3" "BLPOP bltest 0" "*-1"

send_cmd "DEL bl1 bl2 bl3" > /dev/null
send_cmd "RPUSH bl1 first" > /dev/null
send_cmd "RPUSH bl2 second" > /dev/null
run_test_contains "BLPOP-priority1" "BLPOP bl1 bl2 bl3 0" "bl1" "first"
run_test_contains "BLPOP-priority2" "BLPOP bl1 bl2 bl3 0" "bl2" "second"

send_cmd "SET strkey2 notalist" > /dev/null
send_cmd "DEL listkey2" > /dev/null
send_cmd "RPUSH listkey2 val" > /dev/null
run_test_contains "BLPOP-skipwrongtype" "BLPOP strkey2 listkey2 0" "listkey2" "val"

# ==================== Combined Tests (100+) ====================
echo ""
echo "--- Combined Tests ---"

# SET then GET
for i in {1..20}; do
    key="combo_sg_$i"
    val="comboval_$i"
    send_cmd "DEL $key" > /dev/null
    send_cmd "SET $key $val" > /dev/null
    run_test_contains "COMBO-SETGET-$i" "GET $key" "$val"
done

# LPUSH then LPOP (stack behavior)
for i in {1..20}; do
    key="combo_stack_$i"
    send_cmd "DEL $key" > /dev/null
    send_cmd "LPUSH $key first$i" > /dev/null
    send_cmd "LPUSH $key second$i" > /dev/null
    send_cmd "LPUSH $key third$i" > /dev/null
    run_test_contains "COMBO-STACK-$i" "LPOP $key" "third$i"
done

# RPUSH then LPOP (queue behavior)
for i in {1..20}; do
    key="combo_queue_$i"
    send_cmd "DEL $key" > /dev/null
    send_cmd "RPUSH $key first$i" > /dev/null
    send_cmd "RPUSH $key second$i" > /dev/null
    send_cmd "RPUSH $key third$i" > /dev/null
    run_test_contains "COMBO-QUEUE-$i" "LPOP $key" "first$i"
done

# LRANGE after multiple operations
for i in {1..20}; do
    key="combo_range_$i"
    send_cmd "DEL $key" > /dev/null
    send_cmd "RPUSH $key A$i" > /dev/null
    send_cmd "RPUSH $key B$i" > /dev/null
    send_cmd "LPUSH $key Z$i" > /dev/null
    run_test_contains "COMBO-RANGE-$i" "LRANGE $key 0 -1" "Z$i" "A$i" "B$i"
done

# SET, DEL, GET sequence
for i in {1..20}; do
    key="combo_del_$i"
    send_cmd "SET $key value$i" > /dev/null
    send_cmd "DEL $key" > /dev/null
    run_test_contains "COMBO-DEL-$i" "GET $key" '$-1'
done

# LLEN after various list operations
for i in {1..20}; do
    key="combo_len_$i"
    send_cmd "DEL $key" > /dev/null
    send_cmd "RPUSH $key a" > /dev/null
    send_cmd "RPUSH $key b" > /dev/null
    send_cmd "LPUSH $key c" > /dev/null
    send_cmd "LPOP $key" > /dev/null
    run_test_contains "COMBO-LLEN-$i" "LLEN $key" ":2"
done

# Multi-key DEL
for i in {1..10}; do
    k1="mdel_a_$i"
    k2="mdel_b_$i"
    send_cmd "SET $k1 v1" > /dev/null
    send_cmd "SET $k2 v2" > /dev/null
    run_test_contains "COMBO-MULTIDEL-$i" "DEL $k1 $k2" ":2"
done

# LPOP with count after RPUSH
for i in {1..10}; do
    key="combo_popcount_$i"
    send_cmd "DEL $key" > /dev/null
    send_cmd "RPUSH $key x" > /dev/null
    send_cmd "RPUSH $key y" > /dev/null
    send_cmd "RPUSH $key z" > /dev/null
    run_test_contains "COMBO-POPCOUNT-$i" "LPOP $key 2" "*2" "x" "y"
done

# BLPOP with existing data
for i in {1..10}; do
    key="combo_blpop_$i"
    send_cmd "DEL $key" > /dev/null
    send_cmd "RPUSH $key data$i" > /dev/null
    run_test_contains "COMBO-BLPOP-$i" "BLPOP $key 0" "$key" "data$i"
done

# Complex sequence: SET, LPUSH, LRANGE, GET, DEL
for i in {1..10}; do
    skey="combo_complex_s_$i"
    lkey="combo_complex_l_$i"
    send_cmd "DEL $skey $lkey" > /dev/null
    send_cmd "SET $skey stringval$i" > /dev/null
    send_cmd "LPUSH $lkey listval$i" > /dev/null
    run_test_contains "COMBO-COMPLEX-STR-$i" "GET $skey" "stringval$i"
    run_test_contains "COMBO-COMPLEX-LST-$i" "LRANGE $lkey 0 -1" "listval$i"
    send_cmd "DEL $skey $lkey" > /dev/null
done

echo ""
echo "========================================"
echo "  Integration Test Summary"
echo "========================================"
echo "Total:  $TOTAL"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo ""

if [[ $FAILED -eq 0 ]]; then
    echo "✅ ALL TESTS PASSED!"
    exit 0
else
    echo "❌ SOME TESTS FAILED"
    exit 1
fi
