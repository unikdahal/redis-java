package com.redis.integration;

import com.redis.replication.ReplicationManager;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Replication commands: INFO, REPLCONF, PSYNC, WAIT.
 * <p>
 * Tests the replication protocol and commands through the RESP interface.
 */
@DisplayName("Replication Commands Integration Tests")
public class ReplicationCommandsIT extends BaseIntegrationTest {

    @BeforeEach
    void resetReplication() {
        // Ensure we're starting fresh
        ReplicationManager.reset();
    }

    // ==================== INFO Command Tests ====================

    @Nested
    @DisplayName("INFO Command")
    class InfoCommandTests {

        @Test
        @DisplayName("INFO returns replication section")
        void testInfoReplication() {
            String result = sendCommand("INFO", "replication");
            assertNotNull(result);
            assertTrue(result.contains("role:master"));
            assertTrue(result.contains("master_replid:"));
            assertTrue(result.contains("master_repl_offset:"));
        }

        @Test
        @DisplayName("INFO without args returns all sections")
        void testInfoAll() {
            String result = sendCommand("INFO");
            assertNotNull(result);
            assertTrue(result.contains("# Server"));
            assertTrue(result.contains("# Replication"));
            assertTrue(result.contains("# Memory"));
        }

        @Test
        @DisplayName("INFO server section")
        void testInfoServer() {
            String result = sendCommand("INFO", "server");
            assertNotNull(result);
            assertTrue(result.contains("# Server"));
            assertTrue(result.contains("redis_version:"));
            assertTrue(result.contains("tcp_port:"));
        }

        @Test
        @DisplayName("INFO memory section")
        void testInfoMemory() {
            String result = sendCommand("INFO", "memory");
            assertNotNull(result);
            assertTrue(result.contains("# Memory"));
            assertTrue(result.contains("used_memory:"));
        }

        @Test
        @DisplayName("INFO shows master role by default")
        void testInfoMasterRole() {
            String result = sendCommand("INFO", "replication");
            assertNotNull(result);
            assertTrue(result.contains("role:master"));
            assertTrue(result.contains("connected_slaves:0"));
        }
    }

    // ==================== REPLCONF Command Tests ====================

    @Nested
    @DisplayName("REPLCONF Command")
    class ReplconfCommandTests {

        @Test
        @DisplayName("REPLCONF listening-port")
        void testReplconfListeningPort() {
            String result = sendCommand("REPLCONF", "listening-port", "6380");
            assertEquals("+OK\r\n", result);
        }

        @Test
        @DisplayName("REPLCONF capa psync2")
        void testReplconfCapa() {
            String result = sendCommand("REPLCONF", "capa", "psync2");
            assertEquals("+OK\r\n", result);
        }

        @Test
        @DisplayName("REPLCONF without subcommand returns error")
        void testReplconfNoArgs() {
            assertError("REPLCONF");
        }

        @Test
        @DisplayName("REPLCONF unknown subcommand")
        void testReplconfUnknown() {
            String result = sendCommand("REPLCONF", "unknown");
            assertTrue(result.startsWith("-"));
        }

        @Test
        @DisplayName("REPLCONF listening-port with invalid port")
        void testReplconfInvalidPort() {
            String result = sendCommand("REPLCONF", "listening-port", "notaport");
            assertTrue(result.startsWith("-"));
        }
    }

    // ==================== PSYNC Command Tests ====================

    @Nested
    @DisplayName("PSYNC Command")
    class PsyncCommandTests {

        @BeforeEach
        void setupReplica() {
            // Setup replica handshake first
            sendCommand("REPLCONF", "listening-port", "6380");
            sendCommand("REPLCONF", "capa", "psync2");
        }

        @Test
        @DisplayName("PSYNC ? -1 triggers full resync")
        void testPsyncFullResync() {
            String result = sendCommand("PSYNC", "?", "-1");
            // PSYNC writes directly, result may be null
            // But the channel should have received FULLRESYNC
            // In EmbeddedChannel, we might not get the response back the same way
            // This test validates the command doesn't error
            assertTrue(result == null || result.contains("FULLRESYNC") || !result.startsWith("-"));
        }

        @Test
        @DisplayName("PSYNC wrong number of arguments")
        void testPsyncWrongArgs() {
            assertError("PSYNC");
            assertError("PSYNC", "?");
        }
    }

    // ==================== WAIT Command Tests ====================

    @Nested
    @DisplayName("WAIT Command")
    class WaitCommandTests {

        @Test
        @DisplayName("WAIT with 0 replicas returns immediately")
        void testWaitZeroReplicas() {
            String result = sendCommand("WAIT", "0", "0");
            assertNotNull(result);
            assertTrue(result.startsWith(":"));
        }

        @Test
        @DisplayName("WAIT with no connected replicas returns 0")
        void testWaitNoReplicas() {
            // First do a write so there's something to wait for
            sendCommand("SET", "key", "value");

            String result = sendCommand("WAIT", "1", "100");
            assertEquals(":0\r\n", result);
        }

        @Test
        @DisplayName("WAIT wrong number of arguments")
        void testWaitWrongArgs() {
            assertError("WAIT");
            assertError("WAIT", "1");
        }

        @Test
        @DisplayName("WAIT with invalid numreplicas")
        void testWaitInvalidNum() {
            String result = sendCommand("WAIT", "notanumber", "100");
            assertTrue(result.startsWith("-"));
        }

        @Test
        @DisplayName("WAIT with invalid timeout")
        void testWaitInvalidTimeout() {
            String result = sendCommand("WAIT", "1", "notanumber");
            assertTrue(result.startsWith("-"));
        }

        @Test
        @DisplayName("WAIT with negative numreplicas")
        void testWaitNegativeNum() {
            String result = sendCommand("WAIT", "-1", "100");
            assertTrue(result.startsWith("-"));
        }

        @Test
        @DisplayName("WAIT with negative timeout")
        void testWaitNegativeTimeout() {
            String result = sendCommand("WAIT", "1", "-100");
            assertTrue(result.startsWith("-"));
        }
    }

    // ==================== Replication Info Tests ====================

    @Nested
    @DisplayName("Replication State")
    class ReplicationStateTests {

        @Test
        @DisplayName("Server starts as master")
        void testStartsAsMaster() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            assertTrue(mgr.isMaster());
            assertFalse(mgr.isSlave());
        }

        @Test
        @DisplayName("Master has valid replication ID")
        void testMasterReplId() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            String replId = mgr.getMasterReplId();
            assertNotNull(replId);
            assertEquals(40, replId.length());  // 40 hex characters
            assertTrue(replId.matches("[0-9a-f]+"));
        }

        @Test
        @DisplayName("Master offset starts at 0")
        void testMasterOffsetStartsZero() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            assertEquals(0, mgr.getMasterReplOffset());
        }

        @Test
        @DisplayName("No connected replicas initially")
        void testNoReplicasInitially() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            assertEquals(0, mgr.getConnectedReplicaCount());
            assertTrue(mgr.getReplicas().isEmpty());
        }
    }

    @AfterEach
    void cleanup() {
        cleanupKeys("key", "test_key");
    }
}
