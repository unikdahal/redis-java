package com.redis.commands.transaction;

import com.redis.server.RedisCommandHandler;
import com.redis.storage.RedisDatabase;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for MULTI/EXEC/DISCARD transaction commands.
 * Uses Netty's EmbeddedChannel for realistic end-to-end testing.
 * <p>
 * Test Coverage:
 * - Basic MULTI/EXEC flow
 * - MULTI/DISCARD flow
 * - Error handling (EXEC without MULTI, DISCARD without MULTI)
 * - Nested MULTI error
 * - Commands queued and executed atomically
 * - Transaction with errors aborts on EXEC
 * - Empty transaction
 */
@DisplayName("MULTI/EXEC/DISCARD Transaction Tests")
public class TransactionCommandTest {

    private EmbeddedChannel channel;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel(new RedisCommandHandler());
        db = RedisDatabase.getInstance();
        // Clean up test keys
        db.remove("tx_test_key");
        db.remove("tx_test_key2");
        db.remove("tx_counter");
    }

    private String sendCommand(String... args) {
        StringBuilder cmd = new StringBuilder();
        cmd.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            cmd.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        ByteBuf buf = Unpooled.copiedBuffer(cmd.toString(), StandardCharsets.UTF_8);
        channel.writeInbound(buf);
        ByteBuf response = channel.readOutbound();
        return response != null ? response.toString(StandardCharsets.UTF_8) : null;
    }

    @Test
    @DisplayName("MULTI returns OK")
    void testMultiReturnsOk() {
        String result = sendCommand("MULTI");
        assertEquals("+OK\r\n", result);
    }

    @Test
    @DisplayName("Commands are queued after MULTI")
    void testCommandsQueuedAfterMulti() {
        sendCommand("MULTI");

        String result1 = sendCommand("SET", "tx_test_key", "value1");
        assertEquals("+QUEUED\r\n", result1);

        String result2 = sendCommand("GET", "tx_test_key");
        assertEquals("+QUEUED\r\n", result2);

        // Value should not be set yet
        assertNull(db.get("tx_test_key"));
    }

    @Test
    @DisplayName("EXEC executes queued commands and returns results")
    void testExecExecutesQueuedCommands() {
        sendCommand("MULTI");
        sendCommand("SET", "tx_test_key", "hello");
        sendCommand("GET", "tx_test_key");
        sendCommand("SET", "tx_test_key", "world");
        sendCommand("GET", "tx_test_key");

        String result = sendCommand("EXEC");

        // Expected: array of 4 responses
        // *4\r\n+OK\r\n$5\r\nhello\r\n+OK\r\n$5\r\nworld\r\n
        assertTrue(result.startsWith("*4\r\n"), "Should return array of 4 elements");
        assertTrue(result.contains("+OK\r\n"), "Should contain OK for SET");
        assertTrue(result.contains("$5\r\nhello\r\n"), "Should contain 'hello'");
        assertTrue(result.contains("$5\r\nworld\r\n"), "Should contain 'world'");

        // Final value should be "world"
        assertEquals("world", db.get("tx_test_key"));
    }

    @Test
    @DisplayName("DISCARD cancels the transaction")
    void testDiscardCancelsTransaction() {
        db.put("tx_test_key", "original");

        sendCommand("MULTI");
        sendCommand("SET", "tx_test_key", "modified");

        String result = sendCommand("DISCARD");
        assertEquals("+OK\r\n", result);

        // Value should remain unchanged
        assertEquals("original", db.get("tx_test_key"));
    }

    @Test
    @DisplayName("EXEC without MULTI returns error")
    void testExecWithoutMulti() {
        String result = sendCommand("EXEC");
        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("without MULTI"));
    }

    @Test
    @DisplayName("DISCARD without MULTI returns error")
    void testDiscardWithoutMulti() {
        String result = sendCommand("DISCARD");
        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("without MULTI"));
    }

    @Test
    @DisplayName("Nested MULTI returns error")
    void testNestedMulti() {
        sendCommand("MULTI");
        String result = sendCommand("MULTI");
        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("nested"));
    }

    @Test
    @DisplayName("Empty transaction returns empty array")
    void testEmptyTransaction() {
        sendCommand("MULTI");
        String result = sendCommand("EXEC");
        assertEquals("*0\r\n", result);
    }

    @Test
    @DisplayName("Transaction with INCR operations")
    void testTransactionWithIncr() {
        db.put("tx_counter", "0");

        sendCommand("MULTI");
        sendCommand("INCR", "tx_counter");
        sendCommand("INCR", "tx_counter");
        sendCommand("INCR", "tx_counter");
        sendCommand("GET", "tx_counter");

        String result = sendCommand("EXEC");

        // Should return array: [:1, :2, :3, $1\r\n3\r\n]
        assertTrue(result.startsWith("*4\r\n"));
        assertTrue(result.contains(":1\r\n"));
        assertTrue(result.contains(":2\r\n"));
        assertTrue(result.contains(":3\r\n"));

        assertEquals("3", db.get("tx_counter"));
    }

    @Test
    @DisplayName("Unknown command in transaction marks error")
    void testUnknownCommandInTransaction() {
        sendCommand("MULTI");
        String queueResult = sendCommand("INVALIDCMD", "arg");
        assertTrue(queueResult.contains("ERR"));

        String execResult = sendCommand("EXEC");
        assertTrue(execResult.contains("EXECABORT") || execResult.contains("discarded"));
    }

    @Test
    @DisplayName("Can start new transaction after EXEC")
    void testNewTransactionAfterExec() {
        sendCommand("MULTI");
        sendCommand("SET", "tx_test_key", "first");
        sendCommand("EXEC");

        // Should be able to start a new transaction
        String result = sendCommand("MULTI");
        assertEquals("+OK\r\n", result);

        sendCommand("SET", "tx_test_key", "second");
        sendCommand("EXEC");

        assertEquals("second", db.get("tx_test_key"));
    }

    @Test
    @DisplayName("Can start new transaction after DISCARD")
    void testNewTransactionAfterDiscard() {
        sendCommand("MULTI");
        sendCommand("SET", "tx_test_key", "discarded");
        sendCommand("DISCARD");

        // Should be able to start a new transaction
        String result = sendCommand("MULTI");
        assertEquals("+OK\r\n", result);

        sendCommand("SET", "tx_test_key", "new_value");
        sendCommand("EXEC");

        assertEquals("new_value", db.get("tx_test_key"));
    }

    @Test
    @DisplayName("Multiple keys in single transaction")
    void testMultipleKeysInTransaction() {
        sendCommand("MULTI");
        sendCommand("SET", "tx_test_key", "value1");
        sendCommand("SET", "tx_test_key2", "value2");
        sendCommand("GET", "tx_test_key");
        sendCommand("GET", "tx_test_key2");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*4\r\n"));
        assertEquals("value1", db.get("tx_test_key"));
        assertEquals("value2", db.get("tx_test_key2"));
    }

    @Test
    @DisplayName("DEL command in transaction")
    void testDelInTransaction() {
        db.put("tx_test_key", "to_be_deleted");

        sendCommand("MULTI");
        sendCommand("DEL", "tx_test_key");
        sendCommand("GET", "tx_test_key");

        String result = sendCommand("EXEC");

        // Should contain :1 for DEL (1 key deleted) and $-1 for GET (nil)
        assertTrue(result.contains(":1\r\n"));
        assertTrue(result.contains("$-1\r\n"));

        assertNull(db.get("tx_test_key"));
    }

    @Test
    @DisplayName("PING in transaction")
    void testPingInTransaction() {
        sendCommand("MULTI");
        sendCommand("PING");
        sendCommand("PING", "hello");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains("+PONG\r\n"));
        assertTrue(result.contains("$5\r\nhello\r\n"));
    }

    @Test
    @DisplayName("Large transaction with many commands")
    void testLargeTransaction() {
        sendCommand("MULTI");

        // Queue 100 commands
        for (int i = 0; i < 100; i++) {
            String queueResult = sendCommand("SET", "large_tx_key_" + i, "value_" + i);
            assertEquals("+QUEUED\r\n", queueResult);
        }

        String result = sendCommand("EXEC");

        // Should return array of 100 results
        assertTrue(result.startsWith("*100\r\n"));

        // Verify all values were set
        for (int i = 0; i < 100; i++) {
            assertEquals("value_" + i, db.get("large_tx_key_" + i));
        }
    }

    @Test
    @DisplayName("Transaction with INCR on non-existent key")
    void testTransactionIncrNonExistent() {
        db.remove("tx_new_counter");

        sendCommand("MULTI");
        sendCommand("INCR", "tx_new_counter");
        sendCommand("INCR", "tx_new_counter");
        sendCommand("GET", "tx_new_counter");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*3\r\n"));
        assertTrue(result.contains(":1\r\n"));
        assertTrue(result.contains(":2\r\n"));
        assertEquals("2", db.get("tx_new_counter"));
    }

    @Test
    @DisplayName("Transaction with INCR error on wrong type")
    void testTransactionIncrWrongType() {
        db.put("tx_wrong_type", RedisValue.list(new java.util.ArrayList<>()));

        sendCommand("MULTI");
        sendCommand("SET", "tx_test_key", "value");
        sendCommand("INCR", "tx_wrong_type");

        String result = sendCommand("EXEC");

        // Transaction should execute but INCR should return error
        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains("+OK\r\n"));
        assertTrue(result.contains("WRONGTYPE") || result.contains("-ERR"));

        // First command should have succeeded
        assertEquals("value", db.get("tx_test_key"));
    }

    @Test
    @DisplayName("Multiple transactions in sequence")
    void testMultipleTransactionsInSequence() {
        // First transaction
        sendCommand("MULTI");
        sendCommand("SET", "seq_key", "first");
        String result1 = sendCommand("EXEC");
        assertTrue(result1.startsWith("*1\r\n"));

        // Second transaction
        sendCommand("MULTI");
        sendCommand("SET", "seq_key", "second");
        String result2 = sendCommand("EXEC");
        assertTrue(result2.startsWith("*1\r\n"));

        // Third transaction
        sendCommand("MULTI");
        sendCommand("GET", "seq_key");
        String result3 = sendCommand("EXEC");
        assertTrue(result3.contains("$6\r\nsecond\r\n"));

        assertEquals("second", db.get("seq_key"));
    }

    @Test
    @DisplayName("Transaction with same key modified multiple times")
    void testTransactionSameKeyMultipleModifications() {
        sendCommand("MULTI");
        sendCommand("SET", "multi_mod", "v1");
        sendCommand("SET", "multi_mod", "v2");
        sendCommand("SET", "multi_mod", "v3");
        sendCommand("GET", "multi_mod");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*4\r\n"));
        assertTrue(result.contains("$2\r\nv3\r\n"));
        assertEquals("v3", db.get("multi_mod"));
    }

    @Test
    @DisplayName("DISCARD after queueing multiple commands")
    void testDiscardAfterMultipleCommands() {
        db.put("discard_multi_key1", "original1");
        db.put("discard_multi_key2", "original2");

        sendCommand("MULTI");
        sendCommand("SET", "discard_multi_key1", "modified1");
        sendCommand("SET", "discard_multi_key2", "modified2");
        sendCommand("DEL", "discard_multi_key1");

        String result = sendCommand("DISCARD");
        assertEquals("+OK\r\n", result);

        // All values should remain unchanged
        assertEquals("original1", db.get("discard_multi_key1"));
        assertEquals("original2", db.get("discard_multi_key2"));
    }

    @Test
    @DisplayName("Transaction with TYPE command")
    void testTransactionWithTypeCommand() {
        db.put("tx_type_key", "string_value");

        sendCommand("MULTI");
        sendCommand("TYPE", "tx_type_key");
        sendCommand("TYPE", "nonexistent");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains("+string\r\n"));
        assertTrue(result.contains("+none\r\n"));
    }

    @Test
    @DisplayName("Transaction with ECHO command")
    void testTransactionWithEchoCommand() {
        sendCommand("MULTI");
        sendCommand("ECHO", "hello");
        sendCommand("ECHO", "world");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains("$5\r\nhello\r\n"));
        assertTrue(result.contains("$5\r\nworld\r\n"));
    }

    @Test
    @DisplayName("Transaction aborts after command with wrong number of args")
    void testTransactionAbortsAfterWrongArgs() {
        sendCommand("MULTI");
        sendCommand("SET", "key");  // Missing value - should error

        String execResult = sendCommand("EXEC");
        assertTrue(execResult.contains("EXECABORT") || execResult.contains("discarded"));
    }

    @Test
    @DisplayName("Transaction with EXPIRE command")
    void testTransactionWithExpire() {
        db.put("tx_expire_key", "value");

        sendCommand("MULTI");
        sendCommand("EXPIRE", "tx_expire_key", "60");
        sendCommand("GET", "tx_expire_key");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains(":1\r\n"));  // EXPIRE returns 1 for success
        assertTrue(result.contains("$5\r\nvalue\r\n"));
    }

    @Test
    @DisplayName("Empty transaction after DISCARD and new MULTI")
    void testEmptyTransactionAfterDiscard() {
        sendCommand("MULTI");
        sendCommand("SET", "discard_key", "value");
        sendCommand("DISCARD");

        sendCommand("MULTI");
        String result = sendCommand("EXEC");

        assertEquals("*0\r\n", result);
    }

    @Test
    @DisplayName("Transaction state isolated per connection")
    void testTransactionIsolationPerConnection() {
        // Create a second channel to simulate another client
        EmbeddedChannel channel2 = new EmbeddedChannel(new RedisCommandHandler());

        // Start transaction on first channel
        String result1 = sendCommand("MULTI");
        assertEquals("+OK\r\n", result1);

        // Second channel should be able to start its own transaction
        StringBuilder cmd = new StringBuilder();
        cmd.append("*1\r\n$5\r\nMULTI\r\n");
        ByteBuf buf = Unpooled.copiedBuffer(cmd.toString(), StandardCharsets.UTF_8);
        channel2.writeInbound(buf);
        ByteBuf response = channel2.readOutbound();
        String result2 = response != null ? response.toString(StandardCharsets.UTF_8) : null;
        assertEquals("+OK\r\n", result2);

        channel2.close();
    }

    @Test
    @DisplayName("Transaction with only GET commands (read-only)")
    void testReadOnlyTransaction() {
        db.put("readonly_key1", "value1");
        db.put("readonly_key2", "value2");

        sendCommand("MULTI");
        sendCommand("GET", "readonly_key1");
        sendCommand("GET", "readonly_key2");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*2\r\n"));
        assertTrue(result.contains("$6\r\nvalue1\r\n"));
        assertTrue(result.contains("$6\r\nvalue2\r\n"));
    }

    @Test
    @DisplayName("Transaction with GET on non-existent key returns nil")
    void testTransactionGetNonExistent() {
        sendCommand("MULTI");
        sendCommand("GET", "nonexistent_tx_key");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*1\r\n"));
        assertTrue(result.contains("$-1\r\n"));  // Nil reply
    }

    @Test
    @DisplayName("MULTI called twice without EXEC or DISCARD")
    void testMultiCalledTwiceWithoutExecOrDiscard() {
        String result1 = sendCommand("MULTI");
        assertEquals("+OK\r\n", result1);

        String result2 = sendCommand("MULTI");
        assertTrue(result2.contains("ERR"));
        assertTrue(result2.contains("nested"));
    }

    @Test
    @DisplayName("Transaction with alternating SET and GET")
    void testTransactionAlternatingSetGet() {
        sendCommand("MULTI");
        sendCommand("SET", "alt_key", "v1");
        sendCommand("GET", "alt_key");
        sendCommand("SET", "alt_key", "v2");
        sendCommand("GET", "alt_key");

        String result = sendCommand("EXEC");

        assertTrue(result.startsWith("*4\r\n"));
        // Within transaction, GET should return values as they would be after each SET
        assertTrue(result.contains("$2\r\nv1\r\n"));
        assertTrue(result.contains("$2\r\nv2\r\n"));
    }
}