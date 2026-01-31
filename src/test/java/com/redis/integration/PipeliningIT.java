package com.redis.integration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Pipelining and Connection behavior.
 * <p>
 * Tests the ability to send multiple commands in a single request
 * and verify proper handling of concurrent-like operations.
 */
@DisplayName("Pipelining and Connection Integration Tests")
public class PipeliningIT extends BaseIntegrationTest {

    @AfterEach
    void cleanup() {
        for (int i = 0; i < 100; i++) {
            db.remove("pipe_key_" + i);
        }
        cleanupKeys("pipe_test", "pipe_test2", "counter");
    }

    // ==================== Basic Pipelining Tests ====================

    @Nested
    @DisplayName("Basic Pipelining")
    class BasicPipeliningTests {

        @Test
        @DisplayName("Multiple PING commands pipelined")
        void testPipelinedPing() {
            String commands = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
            ByteBuf buf = Unpooled.copiedBuffer(commands, StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            List<String> responses = new ArrayList<>();
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                responses.add(response.toString(StandardCharsets.UTF_8));
            }

            assertEquals(3, responses.size());
            for (String resp : responses) {
                assertEquals("+PONG\r\n", resp);
            }
        }

        @Test
        @DisplayName("SET and GET pipelined")
        void testPipelinedSetGet() {
            String commands =
                "*3\r\n$3\r\nSET\r\n$9\r\npipe_test\r\n$5\r\nhello\r\n" +
                "*2\r\n$3\r\nGET\r\n$9\r\npipe_test\r\n";

            ByteBuf buf = Unpooled.copiedBuffer(commands, StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            ByteBuf resp1 = channel.readOutbound();
            ByteBuf resp2 = channel.readOutbound();

            assertEquals("+OK\r\n", resp1.toString(StandardCharsets.UTF_8));
            assertEquals("$5\r\nhello\r\n", resp2.toString(StandardCharsets.UTF_8));
        }

        @Test
        @DisplayName("Many commands pipelined")
        void testManyCommandsPipelined() {
            StringBuilder commands = new StringBuilder();
            int numCommands = 50;

            // Build 50 SET commands
            for (int i = 0; i < numCommands; i++) {
                String key = "pipe_key_" + i;
                String value = "value_" + i;
                commands.append("*3\r\n$3\r\nSET\r\n$")
                        .append(key.length()).append("\r\n").append(key).append("\r\n$")
                        .append(value.length()).append("\r\n").append(value).append("\r\n");
            }

            ByteBuf buf = Unpooled.copiedBuffer(commands.toString(), StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            int responseCount = 0;
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                assertEquals("+OK\r\n", response.toString(StandardCharsets.UTF_8));
                responseCount++;
            }

            assertEquals(numCommands, responseCount);

            // Verify all values were set
            for (int i = 0; i < numCommands; i++) {
                assertEquals("value_" + i, db.get("pipe_key_" + i));
            }
        }

        @Test
        @DisplayName("Mixed commands pipelined")
        void testMixedCommandsPipelined() {
            String commands =
                "*1\r\n$4\r\nPING\r\n" +
                "*3\r\n$3\r\nSET\r\n$9\r\npipe_test\r\n$5\r\nvalue\r\n" +
                "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n" +
                "*2\r\n$3\r\nGET\r\n$9\r\npipe_test\r\n" +
                "*2\r\n$4\r\nTYPE\r\n$9\r\npipe_test\r\n";

            ByteBuf buf = Unpooled.copiedBuffer(commands, StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            List<String> responses = new ArrayList<>();
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                responses.add(response.toString(StandardCharsets.UTF_8));
            }

            assertEquals(5, responses.size());
            assertEquals("+PONG\r\n", responses.get(0));
            assertEquals("+OK\r\n", responses.get(1));
            assertEquals("$5\r\nhello\r\n", responses.get(2));
            assertEquals("$5\r\nvalue\r\n", responses.get(3));
            assertEquals("+string\r\n", responses.get(4));
        }
    }

    // ==================== INCR Pipelining Tests ====================

    @Nested
    @DisplayName("INCR Pipelining")
    class IncrPipeliningTests {

        @Test
        @DisplayName("Pipelined INCR operations")
        void testPipelinedIncr() {
            db.put("counter", "0");

            StringBuilder commands = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                commands.append("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
            }

            ByteBuf buf = Unpooled.copiedBuffer(commands.toString(), StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            List<String> responses = new ArrayList<>();
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                responses.add(response.toString(StandardCharsets.UTF_8));
            }

            assertEquals(10, responses.size());
            for (int i = 0; i < 10; i++) {
                assertEquals(":" + (i + 1) + "\r\n", responses.get(i));
            }

            assertEquals("10", db.get("counter"));
        }

        @Test
        @DisplayName("INCR on non-existent key pipelined")
        void testPipelinedIncrNewKey() {
            db.remove("counter");

            String commands =
                "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n" +
                "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n" +
                "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";

            ByteBuf buf = Unpooled.copiedBuffer(commands, StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            List<String> responses = new ArrayList<>();
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                responses.add(response.toString(StandardCharsets.UTF_8));
            }

            assertEquals(3, responses.size());
            assertEquals(":1\r\n", responses.get(0));
            assertEquals(":2\r\n", responses.get(1));
            assertEquals(":3\r\n", responses.get(2));
        }
    }

    // ==================== Error Handling in Pipeline ====================

    @Nested
    @DisplayName("Error Handling in Pipeline")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Error in middle of pipeline doesn't stop subsequent commands")
        void testErrorDoesntStopPipeline() {
            String commands =
                "*3\r\n$3\r\nSET\r\n$9\r\npipe_test\r\n$5\r\nvalue\r\n" +
                "*1\r\n$3\r\nGET\r\n" +  // Wrong args - error
                "*2\r\n$3\r\nGET\r\n$9\r\npipe_test\r\n";

            ByteBuf buf = Unpooled.copiedBuffer(commands, StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            List<String> responses = new ArrayList<>();
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                responses.add(response.toString(StandardCharsets.UTF_8));
            }

            assertEquals(3, responses.size());
            assertEquals("+OK\r\n", responses.get(0));
            assertTrue(responses.get(1).startsWith("-"));  // Error
            assertEquals("$5\r\nvalue\r\n", responses.get(2));  // Still works
        }

        @Test
        @DisplayName("Unknown command in pipeline")
        void testUnknownCommandInPipeline() {
            String commands =
                "*1\r\n$4\r\nPING\r\n" +
                "*1\r\n$11\r\nUNKNOWNCMD\r\n" +
                "*1\r\n$4\r\nPING\r\n";

            ByteBuf buf = Unpooled.copiedBuffer(commands, StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            List<String> responses = new ArrayList<>();
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                responses.add(response.toString(StandardCharsets.UTF_8));
            }

            // Should have at least 2 responses (PING responses and/or error)
            assertTrue(responses.size() >= 2, "Expected at least 2 responses, got: " + responses.size());
            // First PING should succeed
            assertEquals("+PONG\r\n", responses.get(0));
            // One response should be an error for the unknown command
            boolean hasError = responses.stream().anyMatch(r -> r.startsWith("-ERR"));
            assertTrue(hasError, "Expected an error response for unknown command");
        }
    }

    // ==================== Fragmented Input Tests ====================

    @Nested
    @DisplayName("Fragmented Input")
    class FragmentedInputTests {

        @Test
        @DisplayName("Command split across multiple writes")
        void testFragmentedCommand() {
            // Send PING in fragments
            channel.writeInbound(Unpooled.copiedBuffer("*1\r\n", StandardCharsets.UTF_8));
            assertNull(channel.readOutbound());  // Not complete yet

            channel.writeInbound(Unpooled.copiedBuffer("$4\r\n", StandardCharsets.UTF_8));
            assertNull(channel.readOutbound());  // Still not complete

            channel.writeInbound(Unpooled.copiedBuffer("PING\r\n", StandardCharsets.UTF_8));

            ByteBuf response = channel.readOutbound();
            assertNotNull(response);
            assertEquals("+PONG\r\n", response.toString(StandardCharsets.UTF_8));
        }

        @Test
        @DisplayName("Multiple commands with fragmentation")
        void testMultipleFragmentedCommands() {
            // First fragment: complete PING + partial SET
            channel.writeInbound(Unpooled.copiedBuffer(
                "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n", StandardCharsets.UTF_8));

            ByteBuf resp1 = channel.readOutbound();
            assertEquals("+PONG\r\n", resp1.toString(StandardCharsets.UTF_8));

            // Second fragment: rest of SET
            channel.writeInbound(Unpooled.copiedBuffer(
                "$9\r\npipe_test\r\n$5\r\nhello\r\n", StandardCharsets.UTF_8));

            ByteBuf resp2 = channel.readOutbound();
            assertEquals("+OK\r\n", resp2.toString(StandardCharsets.UTF_8));

            assertEquals("hello", db.get("pipe_test"));
        }
    }

    // ==================== Connection Independence Tests ====================

    @Nested
    @DisplayName("Connection Independence")
    class ConnectionIndependenceTests {

        @Test
        @DisplayName("Multiple connections can pipeline independently")
        void testMultipleConnectionsPipeline() {
            EmbeddedChannel channel2 = createNewChannel();

            try {
                // Channel 1 sends SET
                channel.writeInbound(Unpooled.copiedBuffer(
                    "*3\r\n$3\r\nSET\r\n$9\r\npipe_test\r\n$2\r\nc1\r\n", StandardCharsets.UTF_8));

                // Channel 2 sends SET to different key
                channel2.writeInbound(Unpooled.copiedBuffer(
                    "*3\r\n$3\r\nSET\r\n$10\r\npipe_test2\r\n$2\r\nc2\r\n", StandardCharsets.UTF_8));

                ByteBuf resp1 = channel.readOutbound();
                ByteBuf resp2 = channel2.readOutbound();

                assertEquals("+OK\r\n", resp1.toString(StandardCharsets.UTF_8));
                assertEquals("+OK\r\n", resp2.toString(StandardCharsets.UTF_8));

                assertEquals("c1", db.get("pipe_test"));
                assertEquals("c2", db.get("pipe_test2"));

            } finally {
                channel2.close();
            }
        }

        @Test
        @DisplayName("Fragmented commands on different connections")
        void testFragmentedOnDifferentConnections() {
            EmbeddedChannel channel2 = createNewChannel();

            try {
                // Start command on channel 1
                channel.writeInbound(Unpooled.copiedBuffer("*1\r\n", StandardCharsets.UTF_8));

                // Complete command on channel 2
                channel2.writeInbound(Unpooled.copiedBuffer(
                    "*1\r\n$4\r\nPING\r\n", StandardCharsets.UTF_8));

                // Channel 2 should respond
                ByteBuf resp2 = channel2.readOutbound();
                assertEquals("+PONG\r\n", resp2.toString(StandardCharsets.UTF_8));

                // Channel 1 should still be waiting
                assertNull(channel.readOutbound());

                // Complete channel 1's command
                channel.writeInbound(Unpooled.copiedBuffer("$4\r\nPING\r\n", StandardCharsets.UTF_8));

                ByteBuf resp1 = channel.readOutbound();
                assertEquals("+PONG\r\n", resp1.toString(StandardCharsets.UTF_8));

            } finally {
                channel2.close();
            }
        }
    }

    // ==================== Stress Tests ====================

    @Nested
    @DisplayName("Stress Tests")
    class StressTests {

        @Test
        @DisplayName("100 pipelined commands")
        void test100PipelinedCommands() {
            StringBuilder commands = new StringBuilder();

            for (int i = 0; i < 100; i++) {
                commands.append("*1\r\n$4\r\nPING\r\n");
            }

            ByteBuf buf = Unpooled.copiedBuffer(commands.toString(), StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            int count = 0;
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                assertEquals("+PONG\r\n", response.toString(StandardCharsets.UTF_8));
                count++;
            }

            assertEquals(100, count);
        }

        @Test
        @DisplayName("Large values pipelined")
        void testLargeValuesPipelined() {
            String largeValue = "x".repeat(10000);

            String commands =
                "*3\r\n$3\r\nSET\r\n$9\r\npipe_test\r\n$" + largeValue.length() + "\r\n" + largeValue + "\r\n" +
                "*2\r\n$3\r\nGET\r\n$9\r\npipe_test\r\n";

            ByteBuf buf = Unpooled.copiedBuffer(commands, StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            ByteBuf resp1 = channel.readOutbound();
            ByteBuf resp2 = channel.readOutbound();

            assertEquals("+OK\r\n", resp1.toString(StandardCharsets.UTF_8));
            assertTrue(resp2.toString(StandardCharsets.UTF_8).contains(largeValue));
        }
    }
}
