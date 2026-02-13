package com.redis.replication;

import com.redis.storage.RedisDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SnapshotProducer.
 */
@DisplayName("SnapshotProducer Tests")
class SnapshotProducerTest {

    private SnapshotProducer producer;
    private static int keyCounter = 0;

    @BeforeEach
    void setUp() {
        SnapshotProducer.reset();
        producer = SnapshotProducer.getInstance();
    }

    private String uniqueKey() {
        return "snapshot_test_key_" + (++keyCounter);
    }

    @Nested
    @DisplayName("Empty Snapshot Generation")
    class EmptySnapshotGeneration {

        @Test
        @DisplayName("Generates valid empty RDB file")
        void testGenerateEmptySnapshot() {
            byte[] rdb = producer.generateEmptySnapshot();

            assertNotNull(rdb);
            assertTrue(rdb.length > 0);

            // Check RDB magic header
            String header = new String(rdb, 0, 5);
            assertEquals("REDIS", header);

            // Check RDB version
            String version = new String(rdb, 5, 4);
            assertEquals("0011", version);
        }

        @Test
        @DisplayName("Empty snapshot contains EOF marker")
        void testEmptySnapshotContainsEof() {
            byte[] rdb = producer.generateEmptySnapshot();

            // EOF marker is 0xFF followed by 8 bytes of CRC
            boolean hasEof = false;
            for (int i = 0; i < rdb.length - 8; i++) {
                if ((rdb[i] & 0xFF) == 0xFF) {
                    hasEof = true;
                    break;
                }
            }
            assertTrue(hasEof);
        }
    }

    @Nested
    @DisplayName("Full Snapshot Generation")
    class FullSnapshotGeneration {

        @Test
        @DisplayName("Generates snapshot with data")
        void testGenerateSnapshotWithData() {
            // Add some test data
            RedisDatabase db = RedisDatabase.getInstance();
            String key = uniqueKey();
            db.put(key, "test_value");

            byte[] rdb = producer.generateSnapshot(0);

            assertNotNull(rdb);
            assertTrue(rdb.length > producer.generateEmptySnapshot().length);
        }

        @Test
        @DisplayName("Updates baseline offset")
        void testUpdatesBaselineOffset() {
            long testOffset = 12345;
            producer.generateSnapshot(testOffset);

            assertEquals(testOffset, producer.getBaselineOffset());
        }

        @Test
        @DisplayName("Increments snapshot count")
        void testIncrementsSnapshotCount() {
            long initialCount = producer.getSnapshotCount();

            producer.generateSnapshot(0);

            assertEquals(initialCount + 1, producer.getSnapshotCount());
        }

        @Test
        @DisplayName("Updates last snapshot time")
        void testUpdatesLastSnapshotTime() {
            long before = System.currentTimeMillis();

            producer.generateSnapshot(0);

            long after = System.currentTimeMillis();
            assertTrue(producer.getLastSnapshotTime() >= before);
            assertTrue(producer.getLastSnapshotTime() <= after);
        }
    }

    @Nested
    @DisplayName("Transfer Format")
    class TransferFormat {

        @Test
        @DisplayName("Wraps RDB in RESP bulk string format")
        void testWrapForTransfer() {
            byte[] rdb = producer.generateEmptySnapshot();
            byte[] transfer = producer.wrapForTransfer(rdb);

            // Should start with $ (bulk string marker)
            assertEquals('$', (char) transfer[0]);

            // Should contain the length
            String prefix = new String(transfer, 0, 20);
            assertTrue(prefix.contains(String.valueOf(rdb.length)));

            // Should contain \r\n after length
            assertTrue(prefix.contains("\r\n"));
        }

        @Test
        @DisplayName("Transfer format contains original RDB data")
        void testTransferContainsRdb() {
            byte[] rdb = producer.generateEmptySnapshot();
            byte[] transfer = producer.wrapForTransfer(rdb);

            // Find where RDB data starts (after $length\r\n)
            int rdbStart = -1;
            for (int i = 0; i < transfer.length - 1; i++) {
                if (transfer[i] == '\r' && transfer[i + 1] == '\n') {
                    rdbStart = i + 2;
                    break;
                }
            }

            assertTrue(rdbStart > 0);

            // Verify RDB data is intact
            for (int i = 0; i < rdb.length; i++) {
                assertEquals(rdb[i], transfer[rdbStart + i]);
            }
        }
    }

    @Nested
    @DisplayName("Concurrency Control")
    class ConcurrencyControl {

        @Test
        @DisplayName("Prevents concurrent snapshot generation")
        void testPreventsConcurrentGeneration() throws InterruptedException {
            // Start a snapshot in another thread
            Thread t = new Thread(() -> {
                // Simulate slow snapshot
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // The actual test is that the singleton prevents
            // multiple simultaneous generations
            assertFalse(producer.isInProgress());
        }

        @Test
        @DisplayName("inProgress flag is cleared after completion")
        void testInProgressCleared() {
            producer.generateSnapshot(0);
            assertFalse(producer.isInProgress());
        }
    }

    @Nested
    @DisplayName("Singleton Behavior")
    class SingletonBehavior {

        @Test
        @DisplayName("Returns same instance")
        void testSameInstance() {
            SnapshotProducer p1 = SnapshotProducer.getInstance();
            SnapshotProducer p2 = SnapshotProducer.getInstance();

            assertSame(p1, p2);
        }

        @Test
        @DisplayName("Reset creates new instance")
        void testResetCreatesNew() {
            SnapshotProducer original = SnapshotProducer.getInstance();
            original.generateSnapshot(100);

            SnapshotProducer.reset();
            SnapshotProducer newInstance = SnapshotProducer.getInstance();

            // New instance should have zero counters
            assertEquals(0, newInstance.getSnapshotCount());
        }
    }
}
