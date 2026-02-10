package com.redis.replication;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Replication Log (Backlog) implementation using a ring buffer.
 * <p>
 * This class provides a bounded, memory-efficient storage for recent replication
 * commands, enabling partial resynchronization (PSYNC) with reconnecting replicas.
 *
 * <h2>Core Design Principles</h2>
 * <ul>
 *   <li><b>Single Writer:</b> Only the master appends to the log</li>
 *   <li><b>Multiple Readers:</b> Replicas can read for partial resync</li>
 *   <li><b>Bounded Memory:</b> Ring buffer with automatic eviction</li>
 *   <li><b>Offset Tracking:</b> Global offsets for replica synchronization</li>
 * </ul>
 *
 * <h2>Memory Layout</h2>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │                    Ring Buffer (1MB default)                 │
 * │  ┌───────────────────────────────────────────────────────┐  │
 * │  │ [old data evicted] ... [valid data] ... [write pos]   │  │
 * │  │        ↑                      ↑             ↑         │  │
 * │  │   firstOffset            readPos       writePos       │  │
 * │  └───────────────────────────────────────────────────────┘  │
 * │                                                              │
 * │  globalOffset = firstOffset + (writePos - firstPos)         │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * <ul>
 *   <li>Append: Single writer (master command execution thread)</li>
 *   <li>Read: Multiple readers (PSYNC handlers) with read lock</li>
 *   <li>Metadata: AtomicLong for lock-free offset queries</li>
 * </ul>
 *
 * <h2>Offset Semantics</h2>
 * <ul>
 *   <li><b>Global Offset:</b> Total bytes ever written (monotonically increasing)</li>
 *   <li><b>First Available Offset:</b> Oldest offset still in buffer</li>
 *   <li><b>Partial Resync:</b> Possible if requestedOffset >= firstAvailable</li>
 * </ul>
 */
public class ReplicationLog {

    // ==================== Configuration ====================

    /** Default backlog size: 1MB */
    public static final int DEFAULT_SIZE = 1024 * 1024;

    /** Minimum allowed backlog size: 64KB */
    public static final int MIN_SIZE = 64 * 1024;

    /** Maximum allowed backlog size: 512MB */
    public static final int MAX_SIZE = 512 * 1024 * 1024;

    // ==================== Storage ====================

    /** The ring buffer holding replication data */
    private final byte[] buffer;

    /** Buffer size (cached for performance) */
    private final int bufferSize;

    // ==================== Offset Tracking ====================

    /**
     * Global replication offset (total bytes ever written).
     * This is the "master_repl_offset" in INFO replication.
     */
    private final AtomicLong globalOffset;

    /**
     * Offset of the first byte still available in the buffer.
     * Requests for offsets below this require full resync.
     */
    private final AtomicLong firstAvailableOffset;

    /**
     * Current write position in the buffer (0 to bufferSize-1).
     */
    private volatile long writePosition;

    // ==================== State ====================

    /** Whether the backlog has ever been written to */
    private volatile boolean active;

    /** Lock for coordinating reads during partial resync */
    private final ReentrantReadWriteLock rwLock;

    // ==================== Statistics ====================

    /** Total bytes written to backlog (may exceed buffer size due to wraparound) */
    private final AtomicLong totalBytesWritten;

    /** Number of times old data was evicted */
    private final AtomicLong evictionCount;

    // ==================== Constructor ====================

    /**
     * Creates a new replication log with default size.
     */
    public ReplicationLog() {
        this(DEFAULT_SIZE);
    }

    /**
     * Creates a new replication log with specified size.
     *
     * @param size Buffer size in bytes (clamped to MIN_SIZE..MAX_SIZE)
     */
    public ReplicationLog(int size) {
        this.bufferSize = Math.max(MIN_SIZE, Math.min(MAX_SIZE, size));
        this.buffer = new byte[bufferSize];

        this.globalOffset = new AtomicLong(0);
        this.firstAvailableOffset = new AtomicLong(0);
        this.writePosition = 0;

        this.active = false;
        this.rwLock = new ReentrantReadWriteLock();

        this.totalBytesWritten = new AtomicLong(0);
        this.evictionCount = new AtomicLong(0);
    }

    // ==================== Write Operations ====================

    /**
     * Appends data to the replication log.
     * <p>
     * This method is designed for single-writer access (the master's command
     * execution thread). It handles buffer wraparound automatically.
     *
     * @param data The RESP-encoded command bytes to append
     * @return The new global offset after appending
     */
    public long append(byte[] data) {
        if (data == null || data.length == 0) {
            return globalOffset.get();
        }

        rwLock.writeLock().lock();
        try {
            active = true;

            // Calculate positions
            int startIdx = (int) (writePosition % bufferSize);

            // Check if we need to evict old data
            if (data.length >= bufferSize) {
                // Data is larger than buffer - only keep the tail
                System.arraycopy(data, data.length - bufferSize, buffer, 0, bufferSize);
                writePosition = bufferSize;
                long newOffset = globalOffset.addAndGet(data.length);
                firstAvailableOffset.set(newOffset - bufferSize);
                evictionCount.incrementAndGet();
                totalBytesWritten.addAndGet(data.length);
                // Return early to avoid the second addAndGet below
                return newOffset;
            } else if (startIdx + data.length <= bufferSize) {
                // No wraparound needed
                System.arraycopy(data, 0, buffer, startIdx, data.length);
                writePosition += data.length;
            } else {
                // Wraparound: write in two parts
                int firstPartLen = bufferSize - startIdx;
                System.arraycopy(data, 0, buffer, startIdx, firstPartLen);
                System.arraycopy(data, firstPartLen, buffer, 0, data.length - firstPartLen);
                writePosition += data.length;
            }

            // Update global offset (only reached for normal-sized data)
            long newOffset = globalOffset.addAndGet(data.length);

            // Update first available offset if we've wrapped
            if (newOffset > bufferSize) {
                long newFirstAvailable = newOffset - bufferSize;
                long currentFirst = firstAvailableOffset.get();
                if (newFirstAvailable > currentFirst) {
                    firstAvailableOffset.set(newFirstAvailable);
                    evictionCount.incrementAndGet();
                }
            }

            totalBytesWritten.addAndGet(data.length);

            return newOffset;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ==================== Read Operations ====================

    /**
     * Checks if partial resync is possible from the given offset.
     *
     * @param fromOffset The offset the replica wants to resume from
     * @return true if the data from that offset is still available
     */
    public boolean canPartialResync(long fromOffset) {
        if (!active) {
            return false;
        }

        long first = firstAvailableOffset.get();
        long current = globalOffset.get();

        return fromOffset >= first && fromOffset <= current;
    }

    /**
     * Gets backlog data from the specified offset.
     * <p>
     * Thread-safe for concurrent reads during partial resync.
     *
     * @param fromOffset The starting offset
     * @return The data bytes, or null if offset is not available
     */
    public byte[] getDataFrom(long fromOffset) {
        rwLock.readLock().lock();
        try {
            if (!canPartialResync(fromOffset)) {
                return null;
            }

            long current = globalOffset.get();
            int dataLen = (int) (current - fromOffset);

            if (dataLen <= 0) {
                return new byte[0];
            }

            byte[] result = new byte[dataLen];

            // Calculate starting position in ring buffer
            int startIdx = (int) (fromOffset % bufferSize);

            if (startIdx + dataLen <= bufferSize) {
                // No wraparound
                System.arraycopy(buffer, startIdx, result, 0, dataLen);
            } else {
                // Wraparound: read in two parts
                int firstPartLen = bufferSize - startIdx;
                System.arraycopy(buffer, startIdx, result, 0, firstPartLen);
                System.arraycopy(buffer, 0, result, firstPartLen, dataLen - firstPartLen);
            }

            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // ==================== Offset Accessors ====================

    /**
     * Gets the current global replication offset.
     *
     * @return Total bytes written to the replication stream
     */
    public long getGlobalOffset() {
        return globalOffset.get();
    }

    /**
     * Gets the first offset still available in the backlog.
     *
     * @return The oldest retrievable offset
     */
    public long getFirstAvailableOffset() {
        return firstAvailableOffset.get();
    }

    /**
     * Sets the global offset (used during replica promotion).
     *
     * @param offset The new global offset
     */
    public void setGlobalOffset(long offset) {
        globalOffset.set(offset);
    }

    // ==================== State Accessors ====================

    /**
     * Checks if the backlog has any data.
     *
     * @return true if data has been written
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Gets the configured buffer size.
     *
     * @return Buffer size in bytes
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Gets the amount of data currently in the buffer.
     *
     * @return Bytes of valid data (0 to bufferSize)
     */
    public long getHistoryLength() {
        long offset = globalOffset.get();
        long first = firstAvailableOffset.get();
        return offset - first;
    }

    // ==================== Statistics ====================

    /**
     * Gets total bytes ever written to the backlog.
     *
     * @return Cumulative byte count
     */
    public long getTotalBytesWritten() {
        return totalBytesWritten.get();
    }

    /**
     * Gets the number of eviction events.
     *
     * @return Eviction count
     */
    public long getEvictionCount() {
        return evictionCount.get();
    }

    // ==================== Reset ====================

    /**
     * Resets the backlog to initial state.
     * <p>
     * <b>Warning:</b> This should only be called during server restart
     * or testing. Active replicas will need full resync after reset.
     */
    public void reset() {
        rwLock.writeLock().lock();
        try {
            globalOffset.set(0);
            firstAvailableOffset.set(0);
            writePosition = 0;
            active = false;
            totalBytesWritten.set(0);
            evictionCount.set(0);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ==================== Debug ====================

    @Override
    public String toString() {
        return String.format("ReplicationLog{size=%d, offset=%d, firstAvailable=%d, histLen=%d, active=%s}",
            bufferSize, globalOffset.get(), firstAvailableOffset.get(), getHistoryLength(), active);
    }
}
