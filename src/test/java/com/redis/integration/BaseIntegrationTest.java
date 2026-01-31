package com.redis.integration;

import com.redis.server.RedisCommandHandler;
import com.redis.storage.RedisDatabase;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Redis integration tests.
 * <p>
 * Provides common functionality for all integration tests including:
 * <ul>
 *   <li>EmbeddedChannel setup and teardown</li>
 *   <li>RESP protocol command sending</li>
 *   <li>Response parsing utilities</li>
 *   <li>Database access and cleanup</li>
 * </ul>
 * <p>
 * All integration test classes should extend this base class to ensure
 * consistent setup/teardown and utility method availability.
 */
public abstract class BaseIntegrationTest {

    protected EmbeddedChannel channel;
    protected RedisDatabase db;

    @BeforeEach
    void baseSetUp() {
        channel = new EmbeddedChannel(new RedisCommandHandler());
        db = RedisDatabase.getInstance();
    }

    @AfterEach
    void baseTearDown() {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    /**
     * Sends a Redis command via RESP protocol and returns the response.
     *
     * @param args command name followed by arguments
     * @return RESP-formatted response string, or null if no response
     */
    protected String sendCommand(String... args) {
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

    /**
     * Sends a command and asserts it returns +OK.
     */
    protected void assertOk(String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertEquals("+OK\r\n", result,
            "Expected OK for command: " + String.join(" ", args));
    }

    /**
     * Sends a command and asserts it returns a specific simple string.
     */
    protected void assertSimpleString(String expected, String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertEquals("+" + expected + "\r\n", result);
    }

    /**
     * Sends a command and asserts it returns a specific integer.
     */
    protected void assertInteger(long expected, String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertEquals(":" + expected + "\r\n", result);
    }

    /**
     * Sends a command and asserts it returns a specific bulk string.
     */
    protected void assertBulkString(String expected, String... args) {
        String result = sendCommand(args);
        if (expected == null) {
            org.junit.jupiter.api.Assertions.assertEquals("$-1\r\n", result);
        } else {
            org.junit.jupiter.api.Assertions.assertEquals(
                "$" + expected.length() + "\r\n" + expected + "\r\n", result);
        }
    }

    /**
     * Sends a command and asserts it returns nil (null bulk string).
     */
    protected void assertNil(String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertEquals("$-1\r\n", result);
    }

    /**
     * Sends a command and asserts the response contains an error.
     */
    protected void assertError(String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertNotNull(result);
        org.junit.jupiter.api.Assertions.assertTrue(result.startsWith("-"),
            "Expected error response, got: " + result);
    }

    /**
     * Sends a command and asserts the response contains a specific error message.
     */
    protected void assertErrorContains(String errorPart, String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertNotNull(result);
        org.junit.jupiter.api.Assertions.assertTrue(result.startsWith("-"),
            "Expected error response, got: " + result);
        org.junit.jupiter.api.Assertions.assertTrue(result.contains(errorPart),
            "Expected error to contain '" + errorPart + "', got: " + result);
    }

    /**
     * Sends a command and asserts it returns an array of a specific size.
     */
    protected String assertArraySize(int expectedSize, String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertNotNull(result);
        org.junit.jupiter.api.Assertions.assertTrue(result.startsWith("*" + expectedSize + "\r\n"),
            "Expected array of size " + expectedSize + ", got: " + result);
        return result;
    }

    /**
     * Sends a command and asserts it returns an empty array.
     */
    protected void assertEmptyArray(String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertEquals("*0\r\n", result);
    }

    /**
     * Sends a command and asserts it returns a null array.
     */
    protected void assertNullArray(String... args) {
        String result = sendCommand(args);
        org.junit.jupiter.api.Assertions.assertEquals("*-1\r\n", result);
    }

    /**
     * Parses a RESP array response into a list of strings.
     * Handles bulk strings and integers in the array.
     */
    protected List<String> parseArrayResponse(String response) {
        List<String> results = new ArrayList<>();
        if (response == null || !response.startsWith("*")) {
            return results;
        }

        String[] lines = response.split("\r\n");
        int i = 1; // Skip the array header
        while (i < lines.length) {
            String line = lines[i];
            if (line.startsWith("$")) {
                int len = Integer.parseInt(line.substring(1));
                if (len == -1) {
                    results.add(null);
                } else {
                    results.add(lines[++i]);
                }
            } else if (line.startsWith(":")) {
                results.add(line.substring(1));
            } else if (line.startsWith("+")) {
                results.add(line.substring(1));
            } else if (line.startsWith("-")) {
                results.add(line);
            }
            i++;
        }
        return results;
    }

    /**
     * Removes a list of keys from the database.
     */
    protected void cleanupKeys(String... keys) {
        for (String key : keys) {
            db.remove(key);
        }
    }

    /**
     * Creates a new EmbeddedChannel for simulating another client connection.
     */
    protected EmbeddedChannel createNewChannel() {
        return new EmbeddedChannel(new RedisCommandHandler());
    }

    /**
     * Sends a command on a specific channel.
     */
    protected String sendCommandOn(EmbeddedChannel ch, String... args) {
        StringBuilder cmd = new StringBuilder();
        cmd.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            cmd.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        ByteBuf buf = Unpooled.copiedBuffer(cmd.toString(), StandardCharsets.UTF_8);
        ch.writeInbound(buf);
        ByteBuf response = ch.readOutbound();
        return response != null ? response.toString(StandardCharsets.UTF_8) : null;
    }
}
