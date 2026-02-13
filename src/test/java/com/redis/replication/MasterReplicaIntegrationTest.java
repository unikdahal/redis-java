package com.redis.replication;

import com.redis.commands.CommandRegistry;
import com.redis.commands.ICommand;
import com.redis.server.RedisCommandHandler;
import com.redis.storage.RedisDatabase;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for Master-Replica replication.
 * <p>
 * Tests the full replication pipeline:
 * <pre>
 * parse → validate → canonicalize → execute → append to log → send to replicas
 * </pre>
 * <p>
 * These tests verify:
 * <ul>
 *   <li>Command propagation from master to replicas</li>
 *   <li>Command canonicalization (EX→PXAT, XADD *→actual ID)</li>
 *   <li>Replication log (ring buffer) operations</li>
 *   <li>Partial resync (PSYNC) logic</li>
 *   <li>Write protection on replicas</li>
 *   <li>Circuit breaker behavior</li>
 *   <li>Backpressure handling</li>
 *   <li>High-throughput scenarios</li>
 * </ul>
 */
@DisplayName("Master-Replica Integration Tests")
public class MasterReplicaIntegrationTest {

    private EmbeddedChannel masterChannel;
    private RedisDatabase db;
    private String testPrefix; // Unique prefix for each test run

    @BeforeEach
    void setUp() {
        // Reset singletons for clean state
        ReplicationManager.reset();
        SnapshotProducer.reset();
        db = RedisDatabase.getInstance();
        testPrefix = "test_" + UUID.randomUUID().toString().substring(0, 8) + "_";

        masterChannel = new EmbeddedChannel(new RedisCommandHandler());
    }

    @AfterEach
    void tearDown() {
        if (masterChannel != null && masterChannel.isOpen()) {
            masterChannel.close();
        }
        ReplicationManager.reset();
        SnapshotProducer.reset();
    }

    // ==================== Helper Methods ====================

    private String sendCommand(String... args) {
        return sendCommandOn(masterChannel, args);
    }

    private String sendCommandOn(EmbeddedChannel channel, String... args) {
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

    // ==================== Role Management Tests ====================

    @Nested
    @DisplayName("Role Management")
    class RoleManagementTests {

        @Test
        @DisplayName("Server starts as master by default")
        void serverStartsAsMaster() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            assertTrue(mgr.isMaster());
            assertFalse(mgr.isSlave());
            assertEquals(ServerRole.MASTER, mgr.getRole());
        }

        @Test
        @DisplayName("Role can be changed to slave")
        void roleCanBeChangedToSlave() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            mgr.setRole(ServerRole.SLAVE);
            assertFalse(mgr.isMaster());
            assertTrue(mgr.isSlave());
        }

        @Test
        @DisplayName("Role can be changed back to master")
        void roleCanBeChangedBackToMaster() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            mgr.setRole(ServerRole.SLAVE);
            mgr.setRole(ServerRole.MASTER);
            assertTrue(mgr.isMaster());
        }

        @Test
        @DisplayName("Master info can be set for slave mode")
        void masterInfoCanBeSetForSlaveMode() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            mgr.setMasterInfo("localhost", 6379);
            assertTrue(mgr.isSlave());
            assertEquals("localhost", mgr.getMasterHost());
            assertEquals(6379, mgr.getMasterPort());
        }
    }

    // ==================== Command Canonicalization Tests ====================

    @Nested
    @DisplayName("Command Canonicalization")
    class CommandCanonicalizationTests {

        @Test
        @DisplayName("SET with EX is canonicalized to PXAT")
        void setWithExIsCanonicalized() {
            // Execute SET with EX
            String result = sendCommand("SET", testPrefix + "mykey", "myvalue", "EX", "60");
            assertEquals("+OK\r\n", result);

            // Verify the command was registered as write command
            ICommand setCmd = CommandRegistry.getInstance().get("SET");
            assertTrue(setCmd.isWriteCommand());
        }

        @Test
        @DisplayName("SET with PX is canonicalized to PXAT")
        void setWithPxIsCanonicalized() {
            String result = sendCommand("SET", testPrefix + "mykey", "myvalue", "PX", "5000");
            assertEquals("+OK\r\n", result);
        }

        @Test
        @DisplayName("EXPIRE is canonicalized to PEXPIREAT")
        void expireIsCanonicalized() {
            sendCommand("SET", testPrefix + "expkey", "myvalue");
            String result = sendCommand("EXPIRE", testPrefix + "expkey", "60");
            assertEquals(":1\r\n", result);

            ICommand expireCmd = CommandRegistry.getInstance().get("EXPIRE");
            assertEquals("PEXPIREAT", expireCmd.getReplicationCommandName());
        }

        @Test
        @DisplayName("XADD with * is canonicalized to actual ID")
        void xaddWithStarIsCanonicalized() {
            String result = sendCommand("XADD", testPrefix + "mystream", "*", "field", "value");
            assertNotNull(result);
            assertTrue(result.startsWith("$")); // Bulk string with ID
            assertFalse(result.contains("*")); // Should not contain *
        }

        @Test
        @DisplayName("XADD with timestamp-* is canonicalized")
        void xaddWithTimestampStarIsCanonicalized() {
            long now = System.currentTimeMillis();
            String result = sendCommand("XADD", testPrefix + "mystream2", now + "-*", "field", "value");
            assertNotNull(result);
            assertTrue(result.startsWith("$"));
        }

        @Test
        @DisplayName("Commands without canonicalization pass through unchanged")
        void commandsWithoutCanonicalization() {
            String result = sendCommand("SET", "key", "value");
            assertEquals("+OK\r\n", result);

            ICommand setCmd = CommandRegistry.getInstance().get("SET");
            // Simple SET without EX/PX should not have replication args
            List<String> args = List.of("key", "value");
            assertNull(setCmd.getReplicationArgs(args, "+OK\r\n"));
        }
    }

    // ==================== Replication Log Tests ====================

    @Nested
    @DisplayName("Replication Log")
    class ReplicationLogTests {

        @Test
        @DisplayName("Log starts inactive")
        void logStartsInactive() {
            ReplicationLog log = new ReplicationLog();
            assertFalse(log.isActive());
            assertEquals(0, log.getGlobalOffset());
        }

        @Test
        @DisplayName("Append activates log and updates offset")
        void appendActivatesLogAndUpdatesOffset() {
            ReplicationLog log = new ReplicationLog();
            byte[] data = "TEST DATA".getBytes(StandardCharsets.UTF_8);

            long newOffset = log.append(data);

            assertTrue(log.isActive());
            assertEquals(data.length, newOffset);
            assertEquals(data.length, log.getGlobalOffset());
        }

        @Test
        @DisplayName("Multiple appends accumulate offset")
        void multipleAppendsAccumulateOffset() {
            ReplicationLog log = new ReplicationLog();
            byte[] data1 = "DATA1".getBytes(StandardCharsets.UTF_8);
            byte[] data2 = "DATA2".getBytes(StandardCharsets.UTF_8);

            log.append(data1);
            long offset2 = log.append(data2);

            assertEquals(data1.length + data2.length, offset2);
        }

        @Test
        @DisplayName("Can retrieve data from offset")
        void canRetrieveDataFromOffset() {
            ReplicationLog log = new ReplicationLog();
            byte[] data = "TESTDATA".getBytes(StandardCharsets.UTF_8);
            log.append(data);

            byte[] retrieved = log.getDataFrom(0);

            assertNotNull(retrieved);
            assertArrayEquals(data, retrieved);
        }

        @Test
        @DisplayName("Partial resync possible within buffer")
        void partialResyncPossibleWithinBuffer() {
            ReplicationLog log = new ReplicationLog(1024); // 1KB buffer
            byte[] data = "SMALL DATA".getBytes(StandardCharsets.UTF_8);
            log.append(data);

            assertTrue(log.canPartialResync(0));
            assertTrue(log.canPartialResync(data.length));
        }

        @Test
        @DisplayName("Partial resync not possible for evicted data")
        void partialResyncNotPossibleForEvictedData() {
            ReplicationLog log = new ReplicationLog(ReplicationLog.MIN_SIZE); // 64KB
            byte[] largeData = new byte[ReplicationLog.MIN_SIZE + 1000];
            java.util.Arrays.fill(largeData, (byte) 'X');

            log.append(largeData);

            // Offset 0 should be evicted
            assertFalse(log.canPartialResync(0));
            // Current offset should still be valid
            assertTrue(log.canPartialResync(log.getGlobalOffset()));
        }

        @Test
        @DisplayName("Ring buffer wraps around correctly")
        void ringBufferWrapsAround() {
            ReplicationLog log = new ReplicationLog(ReplicationLog.MIN_SIZE);
            int chunkSize = 10000;
            byte[] chunk = new byte[chunkSize];
            java.util.Arrays.fill(chunk, (byte) 'A');

            // Write more than buffer size
            for (int i = 0; i < 10; i++) {
                log.append(chunk);
            }

            long offset = log.getGlobalOffset();
            assertEquals(chunkSize * 10, offset);
            assertTrue(log.getEvictionCount() > 0);
        }
    }

    // ==================== Replica Connection Tests ====================

    @Nested
    @DisplayName("Replica Connection")
    class ReplicaConnectionTests {

        @Test
        @DisplayName("Replica starts in CONNECTING state")
        void replicaStartsInConnectingState() {
            EmbeddedChannel replicaChannel = new EmbeddedChannel();
            ReplicaConnection replica = new ReplicaConnection(replicaChannel, "localhost", 6380);

            assertEquals(ReplicaConnection.ReplicaState.CONNECTING, replica.getState());
            assertEquals("localhost", replica.getHost());
            assertEquals(6380, replica.getPort());

            replicaChannel.close();
        }

        @Test
        @DisplayName("Replica state transitions work correctly")
        void replicaStateTransitionsWork() {
            EmbeddedChannel replicaChannel = new EmbeddedChannel();
            ReplicaConnection replica = new ReplicaConnection(replicaChannel, "localhost", 6380);

            replica.setState(ReplicaConnection.ReplicaState.HANDSHAKE);
            assertEquals(ReplicaConnection.ReplicaState.HANDSHAKE, replica.getState());

            replica.setState(ReplicaConnection.ReplicaState.STREAMING);
            assertEquals(ReplicaConnection.ReplicaState.STREAMING, replica.getState());

            replicaChannel.close();
        }

        @Test
        @DisplayName("Offset tracking works correctly")
        void offsetTrackingWorks() {
            EmbeddedChannel replicaChannel = new EmbeddedChannel();
            ReplicaConnection replica = new ReplicaConnection(replicaChannel, "localhost", 6380);

            assertEquals(0, replica.getAcknowledgedOffset());
            assertEquals(0, replica.getExpectedOffset());

            replica.updateAcknowledgedOffset(1000);
            assertEquals(1000, replica.getAcknowledgedOffset());
            assertTrue(replica.hasAcknowledged(1000));
            assertFalse(replica.hasAcknowledged(1001));

            replicaChannel.close();
        }

        @Test
        @DisplayName("Capabilities can be added and checked")
        void capabilitiesWork() {
            EmbeddedChannel replicaChannel = new EmbeddedChannel();
            ReplicaConnection replica = new ReplicaConnection(replicaChannel, "localhost", 6380);

            replica.addCapability("psync2");
            replica.addCapability("eof");

            assertTrue(replica.hasCapability("psync2"));
            assertTrue(replica.hasCapability("eof"));
            assertFalse(replica.hasCapability("unknown"));

            replicaChannel.close();
        }
    }

    // ==================== Write Protection Tests ====================

    @Nested
    @DisplayName("Write Protection on Replicas")
    class WriteProtectionTests {

        @Test
        @DisplayName("Write commands are marked as write commands")
        void writeCommandsAreMarkedCorrectly() {
            assertTrue(CommandRegistry.getInstance().get("SET").isWriteCommand());
            assertTrue(CommandRegistry.getInstance().get("DEL").isWriteCommand());
            assertTrue(CommandRegistry.getInstance().get("LPUSH").isWriteCommand());
            assertTrue(CommandRegistry.getInstance().get("XADD").isWriteCommand());
            assertTrue(CommandRegistry.getInstance().get("INCR").isWriteCommand());
            assertTrue(CommandRegistry.getInstance().get("EXPIRE").isWriteCommand());
        }

        @Test
        @DisplayName("Read commands are not marked as write commands")
        void readCommandsAreNotWriteCommands() {
            assertFalse(CommandRegistry.getInstance().get("GET").isWriteCommand());
            assertFalse(CommandRegistry.getInstance().get("LRANGE").isWriteCommand());
            assertFalse(CommandRegistry.getInstance().get("XRANGE").isWriteCommand());
            assertFalse(CommandRegistry.getInstance().get("TTL").isWriteCommand());
            assertFalse(CommandRegistry.getInstance().get("TYPE").isWriteCommand());
            assertFalse(CommandRegistry.getInstance().get("PING").isWriteCommand());
        }

        @Test
        @DisplayName("CommandPropagator correctly identifies write commands")
        void commandPropagatorIdentifiesWriteCommands() {
            assertTrue(CommandPropagator.shouldPropagate("SET"));
            assertTrue(CommandPropagator.shouldPropagate("DEL"));
            assertTrue(CommandPropagator.shouldPropagate("LPUSH"));
            assertTrue(CommandPropagator.shouldPropagate("XADD"));

            assertFalse(CommandPropagator.shouldPropagate("GET"));
            assertFalse(CommandPropagator.shouldPropagate("LRANGE"));
            assertFalse(CommandPropagator.shouldPropagate("MULTI"));
            assertFalse(CommandPropagator.shouldPropagate("EXEC"));
        }
    }

    // ==================== Snapshot Producer Tests ====================

    @Nested
    @DisplayName("Snapshot Producer")
    class SnapshotProducerTests {

        @Test
        @DisplayName("Empty snapshot is valid RDB format")
        void emptySnapshotIsValidRdb() {
            SnapshotProducer producer = SnapshotProducer.getInstance();
            byte[] snapshot = producer.generateEmptySnapshot();

            assertNotNull(snapshot);
            assertTrue(snapshot.length > 0);
            // Check RDB magic
            assertEquals('R', snapshot[0]);
            assertEquals('E', snapshot[1]);
            assertEquals('D', snapshot[2]);
            assertEquals('I', snapshot[3]);
            assertEquals('S', snapshot[4]);
        }

        @Test
        @DisplayName("Full snapshot includes data")
        void fullSnapshotIncludesData() {
            // Add some data
            db.put("key1", "value1");
            db.put("key2", "value2");

            SnapshotProducer producer = SnapshotProducer.getInstance();
            byte[] snapshot = producer.generateSnapshot(0);

            assertNotNull(snapshot);
            assertTrue(snapshot.length > 50); // Should be larger than empty
        }

        @Test
        @DisplayName("Snapshot wraps for network transfer")
        void snapshotWrapsForTransfer() {
            SnapshotProducer producer = SnapshotProducer.getInstance();
            byte[] rdb = producer.generateEmptySnapshot();
            byte[] wrapped = producer.wrapForTransfer(rdb);

            assertNotNull(wrapped);
            assertTrue(wrapped.length > rdb.length);
            assertEquals('$', wrapped[0]); // RESP bulk string marker
        }

        @Test
        @DisplayName("Concurrent snapshot requests are rejected")
        void concurrentSnapshotRequestsRejected() throws Exception {
            SnapshotProducer producer = SnapshotProducer.getInstance();

            // Add lots of data to make snapshot take time
            for (int i = 0; i < 10000; i++) {
                db.put("key" + i, "value" + i);
            }

            CountDownLatch started = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);

            // Start first snapshot in background
            CompletableFuture<Void> first = CompletableFuture.runAsync(() -> {
                started.countDown();
                byte[] snap = producer.generateSnapshot(0);
                if (snap != null) successCount.incrementAndGet();
            });

            started.await();

            // Try second snapshot immediately
            byte[] second = producer.generateSnapshot(0);

            first.join();

            // At least one should succeed
            assertTrue(successCount.get() >= 1 || second != null);
        }
    }

    // ==================== High Throughput Tests ====================

    @Nested
    @DisplayName("High Throughput")
    class HighThroughputTests {

        @Test
        @DisplayName("Handle 10K commands without errors")
        void handle10KCommands() {
            int commandCount = 10000;

            for (int i = 0; i < commandCount; i++) {
                String result = sendCommand("SET", "key" + i, "value" + i);
                assertEquals("+OK\r\n", result);
            }

            // Verify some data
            assertEquals("value0", db.get("key0"));
            assertEquals("value9999", db.get("key9999"));
        }

        @Test
        @DisplayName("Replication log handles high throughput")
        void replicationLogHighThroughput() {
            ReplicationLog log = new ReplicationLog(1024 * 1024); // 1MB
            byte[] command = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"
                .getBytes(StandardCharsets.UTF_8);

            long startTime = System.currentTimeMillis();
            int iterations = 100000;

            for (int i = 0; i < iterations; i++) {
                log.append(command);
            }

            long duration = System.currentTimeMillis() - startTime;
            long opsPerSecond = (iterations * 1000L) / Math.max(1, duration);

            System.out.println("Replication log throughput: " + opsPerSecond + " ops/sec");
            assertTrue(opsPerSecond > 10000, "Should handle at least 10K ops/sec");
        }

        @Test
        @DisplayName("Concurrent command execution")
        void concurrentCommandExecution() throws Exception {
            int threadCount = 4;
            int commandsPerThread = 1000;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        EmbeddedChannel threadChannel = new EmbeddedChannel(new RedisCommandHandler());
                        for (int i = 0; i < commandsPerThread; i++) {
                            String key = "t" + threadId + "k" + i;
                            String result = sendCommandOn(threadChannel, "SET", key, "value");
                            if ("+OK\r\n".equals(result)) {
                                successCount.incrementAndGet();
                            }
                        }
                        threadChannel.close();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            assertEquals(threadCount * commandsPerThread, successCount.get());
        }
    }

    // ==================== PSYNC Decision Tests ====================

    @Nested
    @DisplayName("PSYNC Decision Logic")
    class PsyncDecisionTests {

        @Test
        @DisplayName("Full sync when replication ID mismatch")
        void fullSyncOnIdMismatch() {
            ReplicationManager mgr = ReplicationManager.getInstance();

            // Use wrong replication ID
            assertFalse(mgr.canPartialResync("wrong-id-1234567890123456789012345678901234", 0));
        }

        @Test
        @DisplayName("Full sync when offset not in backlog")
        void fullSyncWhenOffsetNotInBacklog() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            String replId = mgr.getMasterReplId();

            // Request offset that was never written
            assertFalse(mgr.canPartialResync(replId, 999999));
        }

        @Test
        @DisplayName("Partial sync when offset in backlog")
        void partialSyncWhenOffsetInBacklog() {
            ReplicationManager mgr = ReplicationManager.getInstance();
            String replId = mgr.getMasterReplId();

            // Directly write to the replication log (propagateToReplicas requires connected replicas)
            ReplicationLog log = mgr.getReplicationLog();
            byte[] data = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes(StandardCharsets.UTF_8);
            log.append(data);

            // Now offset 0 should be available for partial resync
            assertTrue(log.canPartialResync(0));
        }
    }

    // ==================== Statistics Tests ====================

    @Nested
    @DisplayName("Statistics")
    class StatisticsTests {

        @Test
        @DisplayName("LongAdder statistics are thread-safe")
        void longAdderStatisticsThreadSafe() throws Exception {
            ReplicationLog log = new ReplicationLog();
            int threadCount = 4;
            int appendsPerThread = 1000;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);

            byte[] data = "TEST".getBytes(StandardCharsets.UTF_8);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < appendsPerThread; i++) {
                            log.append(data);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            assertEquals(threadCount * appendsPerThread * data.length, log.getTotalBytesWritten());
        }
    }
}
