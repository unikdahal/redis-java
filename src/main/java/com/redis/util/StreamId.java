package com.redis.util;

import java.util.Objects;

/**
 * Represents a Redis Stream ID (milliseconds-sequence).
 */
public record StreamId(long time, long sequence) implements Comparable<StreamId> {

    public static final StreamId MIN = new StreamId(0, 0);
    public static final StreamId MAX = new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);

    public static StreamId parse(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Empty stream ID");
        }
        if (id.equals("0")) {
            return MIN;
        }
        String[] parts = id.split("-");
        if (parts.length == 1) {
            try {
                return new StreamId(Long.parseLong(parts[0]), 0);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Stream ID must be a number or <time>-<sequence>");
            }
        }
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid stream ID format. Expected <time>-<sequence>");
        }
        try {
            return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Stream ID parts must be numbers");
        }
    }

    @Override
    public String toString() {
        return time + "-" + sequence;
    }

    @Override
    public int compareTo(StreamId other) {
        if (this.time != other.time) {
            return Long.compare(this.time, other.time);
        }
        return Long.compare(this.sequence, other.sequence);
    }

    public boolean isGreaterThan(StreamId other) {
        return compareTo(other) > 0;
    }
}
