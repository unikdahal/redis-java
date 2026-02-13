package com.redis.replication;

import com.redis.commands.stream.XAddCommand;
import com.redis.commands.string.SetCommand;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for command replication argument rewriting.
 * <p>
 * Ensures that non-deterministic commands are properly canonicalized
 * before being sent to replicas to maintain consistent state.
 */
@DisplayName("Command Propagator Tests")
class CommandPropagatorTest {

    // ==================== shouldPropagate Tests ====================

    @Nested
    @DisplayName("shouldPropagate")
    class ShouldPropagateTests {

        @Test
        @DisplayName("Write commands should be propagated")
        void writeCommandsShouldPropagate() {
            assertTrue(CommandPropagator.shouldPropagate("SET"));
            assertTrue(CommandPropagator.shouldPropagate("DEL"));
            assertTrue(CommandPropagator.shouldPropagate("LPUSH"));
            assertTrue(CommandPropagator.shouldPropagate("XADD"));
            assertTrue(CommandPropagator.shouldPropagate("INCR"));
            assertTrue(CommandPropagator.shouldPropagate("EXPIRE"));
        }

        @Test
        @DisplayName("Read commands should not be propagated")
        void readCommandsShouldNotPropagate() {
            assertFalse(CommandPropagator.shouldPropagate("GET"));
            assertFalse(CommandPropagator.shouldPropagate("LRANGE"));
            assertFalse(CommandPropagator.shouldPropagate("XREAD"));
            assertFalse(CommandPropagator.shouldPropagate("TTL"));
            assertFalse(CommandPropagator.shouldPropagate("TYPE"));
            assertFalse(CommandPropagator.shouldPropagate("PING"));
        }

        @Test
        @DisplayName("Transaction commands should not be propagated")
        void transactionCommandsShouldNotPropagate() {
            assertFalse(CommandPropagator.shouldPropagate("MULTI"));
            assertFalse(CommandPropagator.shouldPropagate("EXEC"));
            assertFalse(CommandPropagator.shouldPropagate("DISCARD"));
        }
    }

    // ==================== buildRespArray Tests ====================

    @Nested
    @DisplayName("buildRespArray")
    class BuildRespArrayTests {

        @Test
        @DisplayName("Builds correct RESP for simple command")
        void buildsCorrectRespForSimpleCommand() {
            String resp = CommandPropagator.buildRespArray("SET", List.of("key", "value"));
            assertEquals("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", resp);
        }

        @Test
        @DisplayName("Builds correct RESP for command with no args")
        void buildsCorrectRespForNoArgs() {
            String resp = CommandPropagator.buildRespArray("PING", List.of());
            assertEquals("*1\r\n$4\r\nPING\r\n", resp);
        }

        @Test
        @DisplayName("Builds correct RESP for command with multiple args")
        void buildsCorrectRespForMultipleArgs() {
            String resp = CommandPropagator.buildRespArray("XADD",
                List.of("stream", "123-0", "field1", "value1", "field2", "value2"));
            assertEquals("*7\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$5\r\n123-0\r\n" +
                "$6\r\nfield1\r\n$6\r\nvalue1\r\n$6\r\nfield2\r\n$6\r\nvalue2\r\n", resp);
        }
    }

    // ==================== XAddCommand Replication Args Tests ====================

    @Nested
    @DisplayName("XAddCommand getReplicationArgs")
    class XAddReplicationArgsTests {

        private final XAddCommand cmd = new XAddCommand();

        @Test
        @DisplayName("Returns null for explicit ID (no rewriting needed)")
        void returnsNullForExplicitId() {
            List<String> args = List.of("stream", "123-0", "field", "value");
            String response = "$5\r\n123-0\r\n";

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);
            assertNull(replicationArgs);
        }

        @Test
        @DisplayName("Rewrites * to actual generated ID")
        void rewritesAutoId() {
            List<String> args = List.of("stream", "*", "field", "value");
            String response = "$15\r\n1706745600000-0\r\n";

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);

            assertNotNull(replicationArgs);
            assertEquals(4, replicationArgs.size());
            assertEquals("stream", replicationArgs.get(0));
            assertEquals("1706745600000-0", replicationArgs.get(1)); // Rewritten ID
            assertEquals("field", replicationArgs.get(2));
            assertEquals("value", replicationArgs.get(3));
        }

        @Test
        @DisplayName("Rewrites timestamp-* to actual generated ID")
        void rewritesPartialId() {
            List<String> args = List.of("stream", "1706745600000-*", "field", "value");
            String response = "$15\r\n1706745600000-5\r\n";

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);

            assertNotNull(replicationArgs);
            assertEquals("1706745600000-5", replicationArgs.get(1)); // Rewritten ID
        }

        @Test
        @DisplayName("Returns null for error response")
        void returnsNullForErrorResponse() {
            List<String> args = List.of("stream", "*", "field", "value");
            String response = "-ERR some error\r\n";

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);
            assertNull(replicationArgs);
        }

        @Test
        @DisplayName("Preserves all field-value pairs")
        void preservesAllFieldValuePairs() {
            List<String> args = List.of("stream", "*", "f1", "v1", "f2", "v2", "f3", "v3");
            String response = "$15\r\n1706745600000-0\r\n";

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);

            assertNotNull(replicationArgs);
            assertEquals(8, replicationArgs.size());
            assertEquals("f1", replicationArgs.get(2));
            assertEquals("v1", replicationArgs.get(3));
            assertEquals("f2", replicationArgs.get(4));
            assertEquals("v2", replicationArgs.get(5));
            assertEquals("f3", replicationArgs.get(6));
            assertEquals("v3", replicationArgs.get(7));
        }
    }

    // ==================== SetCommand Replication Args Tests ====================

    @Nested
    @DisplayName("SetCommand getReplicationArgs")
    class SetReplicationArgsTests {

        private final SetCommand cmd = new SetCommand();

        @Test
        @DisplayName("Returns null for SET without expiry")
        void returnsNullForSetWithoutExpiry() {
            List<String> args = List.of("key", "value");

            // Execute to populate thread-local
            cmd.execute(args, null);

            List<String> replicationArgs = cmd.getReplicationArgs(args, "+OK\r\n");
            assertNull(replicationArgs);
        }

        @Test
        @DisplayName("Returns null for failed SET (NX condition not met)")
        void returnsNullForFailedSet() {
            List<String> args = List.of("key", "value", "EX", "60");

            // Response indicates failure
            List<String> replicationArgs = cmd.getReplicationArgs(args, "$-1\r\n");
            assertNull(replicationArgs);
        }

        @Test
        @DisplayName("Rewrites EX to PXAT")
        void rewritesExToPxat() {
            List<String> args = List.of("key", "value", "EX", "60");

            // Execute to populate thread-local
            String response = cmd.execute(args, null);

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);

            assertNotNull(replicationArgs);
            assertEquals(4, replicationArgs.size());
            assertEquals("key", replicationArgs.get(0));
            assertEquals("value", replicationArgs.get(1));
            assertEquals("PXAT", replicationArgs.get(2));

            // The PXAT value should be approximately now + 60000ms
            long pxat = Long.parseLong(replicationArgs.get(3));
            long now = System.currentTimeMillis();
            assertTrue(pxat > now && pxat <= now + 61000,
                "PXAT should be ~60 seconds from now");
        }

        @Test
        @DisplayName("Rewrites PX to PXAT")
        void rewritesPxToPxat() {
            List<String> args = List.of("key", "value", "PX", "5000");

            // Execute to populate thread-local
            String response = cmd.execute(args, null);

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);

            assertNotNull(replicationArgs);
            assertEquals("PXAT", replicationArgs.get(2));

            long pxat = Long.parseLong(replicationArgs.get(3));
            long now = System.currentTimeMillis();
            assertTrue(pxat > now && pxat <= now + 6000);
        }

        @Test
        @DisplayName("Preserves NX flag with expiry rewrite")
        void preservesNxFlagWithExpiryRewrite() {
            // Use a unique key that doesn't exist - NX will succeed
            List<String> args = List.of("nx_test_key", "value", "NX", "EX", "60");

            // Execute to populate thread-local
            String response = cmd.execute(args, null);
            assertEquals("+OK\r\n", response, "NX should succeed for new key");

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);

            assertNotNull(replicationArgs);
            assertEquals(5, replicationArgs.size());
            assertEquals("nx_test_key", replicationArgs.get(0));
            assertEquals("value", replicationArgs.get(1));
            assertEquals("NX", replicationArgs.get(2));
            assertEquals("PXAT", replicationArgs.get(3));
        }

        @Test
        @DisplayName("Preserves XX flag with expiry rewrite")
        void preservesXxFlagWithExpiryRewrite() {
            // First set the key so XX condition can be met
            cmd.execute(List.of("xx_test_key", "existing"), null);

            List<String> args = List.of("xx_test_key", "value", "XX", "PX", "5000");
            String response = cmd.execute(args, null);
            assertEquals("+OK\r\n", response, "XX should succeed for existing key");

            List<String> replicationArgs = cmd.getReplicationArgs(args, response);

            assertNotNull(replicationArgs);
            assertTrue(replicationArgs.contains("XX"));
            assertTrue(replicationArgs.contains("PXAT"));
        }
    }

    // ==================== isWriteCommand Tests ====================

    @Nested
    @DisplayName("isWriteCommand")
    class IsWriteCommandTests {

        @Test
        @DisplayName("SetCommand is a write command")
        void setIsWriteCommand() {
            assertTrue(new SetCommand().isWriteCommand());
        }

        @Test
        @DisplayName("XAddCommand is a write command")
        void xaddIsWriteCommand() {
            assertTrue(new XAddCommand().isWriteCommand());
        }
    }
}
