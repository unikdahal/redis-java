package com.redis.util;

import java.util.Objects;

/**
 * Value Object representing a Redis Stream ID.
 * <p>
 * <b>Format:</b> {@code <millisecondsTime>-<sequenceNumber>}
 * <p>
 * <b>Role in Architecture:</b>
 * This class serves as the <b>Key</b> in the {@code ConcurrentSkipListMap} backing the Stream data structure.
 * It implements {@code Comparable} to ensure that the Map keeps messages strictly ordered by time.
 * <p>
 * <b>Why Record?</b>
 * As a Map Key, this object MUST be immutable. Records provide immutability,
 * equals(), and hashCode() out of the box, preventing subtle hashing bugs.
 */
public record StreamId(long time, long sequence) implements Comparable<StreamId> {

    // Sentinel values useful for Range Queries (XRANGE)
    public static final StreamId MIN = new StreamId(0, 0);
    public static final StreamId MAX = new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);

    /**
     * Parses a string ID into a StreamId object.
     * <p>
     * <b>Supported Formats:</b>
     * <ul>
     * <li>{@code "123-456"} -> Time: 123, Seq: 456</li>
     * <li>{@code "123"} -> Time: 123, Seq: 0 (Implicit sequence 0)</li>
     * </ul>
     *
     * @param id The string representation of the ID.
     * @return The parsed immutable StreamId.
     * @throws IllegalArgumentException if format is invalid.
     */
    public static StreamId parse(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Empty stream ID");
        }

        // Edge case: "0" usually implies 0-0
        if (id.equals("0")) {
            return MIN;
        }

        String[] parts = id.split("-");

        // Case 1: "123" (Time only provided)
        if (parts.length == 1) {
            try {
                return new StreamId(Long.parseLong(parts[0]), 0);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Stream ID must be a number or <time>-<sequence>");
            }
        }

        // Case 2: "123-456" (Full ID)
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid stream ID format. Expected <time>-<sequence>");
        }

        try {
            return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Stream ID parts must be numbers");
        }
    }

    /**
     * Converts back to the Redis standard string format {@code "time-sequence"}.
     */
    @Override
    public String toString() {
        return time + "-" + sequence;
    }

    /**
     * Defines the natural sort order for Stream IDs.
     * <p>
     * <b>Logic:</b>
     * 1. Compare timestamps.
     * 2. If timestamps are identical, compare sequence numbers.
     * <p>
     * This ensures strict chronological ordering in the SkipListMap.
     */
    @Override
    public int compareTo(StreamId other) {
        // Optimization: Check time first as it varies most often
        if (this.time != other.time) {
            // Returns -1 if this < other, 1 if this > other
            return Long.compare(this.time, other.time);
        }
        // Fallback: If same millisecond, use sequence
        return Long.compare(this.sequence, other.sequence);
    }

    /**
     * Helper method for readability in validation logic.
     * Equivalent to {@code this.compareTo(other) > 0}.
     */
    public boolean isGreaterThan(StreamId other) {
        return compareTo(other) > 0;
    }
}