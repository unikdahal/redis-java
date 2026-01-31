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
}
