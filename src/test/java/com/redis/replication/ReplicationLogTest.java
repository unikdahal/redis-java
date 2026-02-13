package com.redis.replication;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ReplicationLog.
 * <p>
 * Tests cover:
 * <ul>
 *   <li>Basic append and read operations</li>
 *   <li>Ring buffer wraparound behavior</li>
 *   <li>Partial resync offset validation</li>
 *   <li>Thread safety under concurrent access</li>
 *   <li>Edge cases and error conditions</li>
 * </ul>
 */
@DisplayName("ReplicationLog Tests")
class ReplicationLogTest {

    private ReplicationLog log;

    @BeforeEach
    void setUp() {
        // Use minimum buffer size for testing
        log = new ReplicationLog(ReplicationLog.MIN_SIZE);
    }

    @Nested
    @DisplayName("Basic Operations")
    class BasicOperations {

        @Test
        @DisplayName("Initial state is inactive with zero offset")
        void testInitialState() {
            assertFalse(log.isActive());
            assertEquals(0, log.getGlobalOffset());
            assertEquals(0, log.getFirstAvailableOffset());
        }

        @Test
        @DisplayName("Append activates the log")
        void testAppendActivates() {
            log.append("test".getBytes(StandardCharsets.UTF_8));
            assertTrue(log.isActive());
        }

        @Test
        @DisplayName("Append updates global offset correctly")
        void testAppendUpdatesOffset() {
            byte[] data1 = "hello".getBytes(StandardCharsets.UTF_8);
            byte[] data2 = "world".getBytes(StandardCharsets.UTF_8);

            long offset1 = log.append(data1);
            assertEquals(5, offset1);
            assertEquals(5, log.getGlobalOffset());

            long offset2 = log.append(data2);
            assertEquals(10, offset2);
            assertEquals(10, log.getGlobalOffset());
        }

        @Test
        @DisplayName("Append with null or empty data returns current offset")
        void testAppendEmptyData() {
            log.append("data".getBytes(StandardCharsets.UTF_8));
            long offset = log.getGlobalOffset();

            assertEquals(offset, log.append(null));
            assertEquals(offset, log.append(new byte[0]));
        }

        @Test
        @DisplayName("Can read data after append")
        void testReadAfterAppend() {
            byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
            log.append(data);

            byte[] result = log.getDataFrom(0);
            assertNotNull(result);
            assertArrayEquals(data, result);
        }
    }

    @Nested
    @DisplayName("Partial Resync Validation")
    class PartialResyncValidation {

        @Test
        @DisplayName("canPartialResync returns false when log is empty")
        void testCanPartialResyncEmpty() {
            assertFalse(log.canPartialResync(0));
        }

        @Test
        @DisplayName("canPartialResync returns true for valid offset")
        void testCanPartialResyncValid() {
            log.append("test data".getBytes(StandardCharsets.UTF_8));

            assertTrue(log.canPartialResync(0));
            assertTrue(log.canPartialResync(5));
        }

        @Test
        @DisplayName("canPartialResync returns false for offset beyond current")
        void testCanPartialResyncBeyondCurrent() {
            log.append("test".getBytes(StandardCharsets.UTF_8));
            assertFalse(log.canPartialResync(100));
        }

        @Test
        @DisplayName("canPartialResync returns false for evicted offset")
        void testCanPartialResyncEvicted() {
            // Fill buffer past capacity to trigger eviction
            // Buffer is MIN_SIZE (64KB), so we need to write more than that
            byte[] data = new byte[40 * 1024]; // 40KB chunks
            log.append(data);
            log.append(data); // Total 80KB, which exceeds 64KB buffer

            // Offset 0 should no longer be available after eviction
            assertFalse(log.canPartialResync(0));
        }
    }

    @Nested
    @DisplayName("Ring Buffer Wraparound")
    class RingBufferWraparound {

        @Test
        @DisplayName("Handles wraparound correctly")
        void testWraparound() {
            // Use MIN_SIZE buffer
            ReplicationLog smallLog = new ReplicationLog(ReplicationLog.MIN_SIZE);
            int bufferSize = smallLog.getBufferSize();

            // Write data that will wrap around (more than buffer size)
            byte[] data1 = new byte[bufferSize / 2 + 1000];
            java.util.Arrays.fill(data1, (byte) 'A');
            smallLog.append(data1);

            byte[] data2 = new byte[bufferSize / 2 + 1000];
            java.util.Arrays.fill(data2, (byte) 'B');
            smallLog.append(data2);

            // First write should be partially or fully evicted
            assertFalse(smallLog.canPartialResync(0));

            // Some of the second write should be available
            assertTrue(smallLog.canPartialResync(smallLog.getFirstAvailableOffset()));
        }

        @Test
        @DisplayName("Can read across wraparound boundary")
        void testReadAcrossWraparound() {
            ReplicationLog smallLog = new ReplicationLog(ReplicationLog.MIN_SIZE);
            int bufferSize = smallLog.getBufferSize();

            // Fill to near capacity
            byte[] filler = new byte[bufferSize - 100];
            java.util.Arrays.fill(filler, (byte) 'X');
            smallLog.append(filler);

            // Write data that will wrap
            byte[] wrapData = "wrap".getBytes(StandardCharsets.UTF_8);
            smallLog.append(wrapData);

            // Read the wrapped data - it should be at the end
            long startOffset = bufferSize - 100;
            byte[] result = smallLog.getDataFrom(startOffset);
            assertNotNull(result);
            assertArrayEquals(wrapData, result);
        }

        @Test
        @DisplayName("History length is accurate after wraparound")
        void testHistoryLengthAfterWraparound() {
            ReplicationLog smallLog = new ReplicationLog(ReplicationLog.MIN_SIZE);
            int bufferSize = smallLog.getBufferSize();

            // Write more than buffer size
            byte[] data = new byte[bufferSize / 2 + 1000];
            smallLog.append(data);
            smallLog.append(data);
            smallLog.append(data); // Total is about 1.5x buffer size

            // History length should be at most buffer size
            assertTrue(smallLog.getHistoryLength() <= bufferSize);
        }
    }

    @Nested
    @DisplayName("Data Retrieval")
    class DataRetrieval {

        @Test
        @DisplayName("getDataFrom returns null for invalid offset")
        void testGetDataFromInvalid() {
            log.append("test".getBytes(StandardCharsets.UTF_8));

            assertNull(log.getDataFrom(100)); // Beyond current
            assertNull(log.getDataFrom(-1));  // Negative (wraps to high value)
        }

        @Test
        @DisplayName("getDataFrom returns empty array at current offset")
        void testGetDataFromCurrent() {
            log.append("test".getBytes(StandardCharsets.UTF_8));

            byte[] result = log.getDataFrom(log.getGlobalOffset());
            assertNotNull(result);
            assertEquals(0, result.length);
        }

        @Test
        @DisplayName("getDataFrom returns correct partial data")
        void testGetDataFromPartial() {
            log.append("hello".getBytes(StandardCharsets.UTF_8));
            log.append("world".getBytes(StandardCharsets.UTF_8));

            byte[] result = log.getDataFrom(5);
            assertNotNull(result);
            assertArrayEquals("world".getBytes(StandardCharsets.UTF_8), result);
        }
    }

    @Nested
    @DisplayName("Statistics")
    class Statistics {

        @Test
        @DisplayName("Tracks total bytes written")
        void testTotalBytesWritten() {
            log.append(new byte[100]);
            log.append(new byte[200]);

            assertEquals(300, log.getTotalBytesWritten());
        }

        @Test
        @DisplayName("Tracks eviction count")
        void testEvictionCount() {
            int bufferSize = log.getBufferSize();

            assertEquals(0, log.getEvictionCount());

            // Force eviction by writing more than buffer size
            byte[] data = new byte[bufferSize / 2 + 1000];
            log.append(data);
            log.append(data); // Should trigger eviction

            assertTrue(log.getEvictionCount() > 0);
        }

        @Test
        @DisplayName("Buffer size respects minimum")
        void testBufferSizeMinimum() {
            ReplicationLog customLog = new ReplicationLog(100); // Request too small
            assertEquals(ReplicationLog.MIN_SIZE, customLog.getBufferSize());
        }

        @Test
        @DisplayName("Buffer size is clamped to valid range")
        void testBufferSizeClamped() {
            // Too small - should be clamped to MIN_SIZE
            ReplicationLog tooSmall = new ReplicationLog(100);
            assertEquals(ReplicationLog.MIN_SIZE, tooSmall.getBufferSize());

            // Too large - should be clamped to MAX_SIZE
            ReplicationLog tooLarge = new ReplicationLog(Integer.MAX_VALUE);
            assertEquals(ReplicationLog.MAX_SIZE, tooLarge.getBufferSize());
        }
    }

    @Nested
    @DisplayName("Reset Functionality")
    class ResetFunctionality {

        @Test
        @DisplayName("Reset clears all state")
        void testReset() {
            log.append("test data".getBytes(StandardCharsets.UTF_8));
            assertTrue(log.isActive());
            assertTrue(log.getGlobalOffset() > 0);

            log.reset();

            assertFalse(log.isActive());
            assertEquals(0, log.getGlobalOffset());
            assertEquals(0, log.getFirstAvailableOffset());
            assertEquals(0, log.getTotalBytesWritten());
        }
    }

    @Nested
    @DisplayName("Thread Safety")
    class ThreadSafety {

        @Test
        @DisplayName("Concurrent appends maintain data integrity")
        void testConcurrentAppends() throws InterruptedException {
            int numThreads = 10;
            int appendsPerThread = 100;
            byte[] data = "test".getBytes(StandardCharsets.UTF_8);

            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < appendsPerThread; j++) {
                        log.append(data);
                    }
                });
            }

            for (Thread t : threads) t.start();
            for (Thread t : threads) t.join();

            // Total bytes should be consistent
            long expectedTotal = (long) numThreads * appendsPerThread * data.length;
            assertEquals(expectedTotal, log.getTotalBytesWritten());
        }
    }

    @Test
    @DisplayName("toString provides useful debug info")
    void testToString() {
        log.append("test".getBytes(StandardCharsets.UTF_8));
        String str = log.toString();

        assertTrue(str.contains("ReplicationLog"));
        assertTrue(str.contains("offset="));
        assertTrue(str.contains("active=true"));
    }
}
