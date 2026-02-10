package com.redis.replication;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Snapshot Producer for creating point-in-time database snapshots.
 * <p>
 * This class is responsible for producing consistent database snapshots
 * for full resynchronization with replicas. It operates independently
 * of live mutations to ensure data consistency.
 *
 * <h2>Design Principles</h2>
 * <ul>
 *   <li><b>Isolation:</b> Snapshot captures point-in-time state</li>
 *   <li><b>Non-blocking:</b> Does not block normal operations</li>
 *   <li><b>Memory-efficient:</b> Streams data when possible</li>
 *   <li><b>Consistent:</b> Uses copy-on-write semantics</li>
 * </ul>
 *
 * <h2>Snapshot Types</h2>
 * <ul>
 *   <li><b>Empty RDB:</b> For new replicas (no data)</li>
 *   <li><b>Full RDB:</b> Complete database serialization</li>
 *   <li><b>Incremental:</b> Changes since last snapshot (future)</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <ul>
 *   <li>Snapshot generation can run concurrently with writes</li>
 *   <li>Uses RedisDatabase's atomic iteration</li>
 *   <li>State tracking via AtomicBoolean/AtomicLong</li>
 * </ul>
 */
public class SnapshotProducer {

    // ==================== RDB Constants ====================

    /** RDB file magic string */
    private static final byte[] RDB_MAGIC = "REDIS".getBytes(StandardCharsets.US_ASCII);

    /** RDB version (Redis 7.x compatible) */
    private static final byte[] RDB_VERSION = "0011".getBytes(StandardCharsets.US_ASCII);

    // RDB Opcodes
    private static final byte RDB_OPCODE_AUX = (byte) 0xFA;
    private static final byte RDB_OPCODE_SELECTDB = (byte) 0xFE;
    private static final byte RDB_OPCODE_RESIZEDB = (byte) 0xFB;
    private static final byte RDB_OPCODE_EXPIRETIME_MS = (byte) 0xFC;
    private static final byte RDB_OPCODE_EOF = (byte) 0xFF;

    // RDB Value Types
    private static final byte RDB_TYPE_STRING = 0;
    private static final byte RDB_TYPE_LIST = 1;
    private static final byte RDB_TYPE_STREAM = 15;

    // ==================== State ====================

    /** Whether a snapshot is currently being produced */
    private final AtomicBoolean inProgress;

    /** Offset at which the current snapshot started */
    private final AtomicLong baselineOffset;

    /** Timestamp of last snapshot */
    private final AtomicLong lastSnapshotTime;

    /** Total snapshots produced */
    private final AtomicLong snapshotCount;

    // ==================== Singleton ====================

    private static volatile SnapshotProducer INSTANCE;
    private static final Object INIT_LOCK = new Object();

    private SnapshotProducer() {
        this.inProgress = new AtomicBoolean(false);
        this.baselineOffset = new AtomicLong(0);
        this.lastSnapshotTime = new AtomicLong(0);
        this.snapshotCount = new AtomicLong(0);
    }

    public static SnapshotProducer getInstance() {
        SnapshotProducer instance = INSTANCE;
        if (instance == null) {
            synchronized (INIT_LOCK) {
                instance = INSTANCE;
                if (instance == null) {
                    INSTANCE = instance = new SnapshotProducer();
                }
            }
        }
        return instance;
    }

    // ==================== Snapshot Generation ====================

    /**
     * Generates a complete RDB snapshot of the current database state.
     * <p>
     * This method captures a point-in-time snapshot suitable for
     * full resynchronization with replicas.
     *
     * @param replicationOffset The current replication offset (for tracking)
     * @return RDB file bytes, or null if snapshot already in progress
     */
    public byte[] generateSnapshot(long replicationOffset) {
        // Prevent concurrent snapshot generation
        if (!inProgress.compareAndSet(false, true)) {
            System.out.println("[Snapshot] Snapshot already in progress");
            return null;
        }

        try {
            baselineOffset.set(replicationOffset);
            long startTime = System.currentTimeMillis();

            byte[] snapshot = createRdbSnapshot();

            lastSnapshotTime.set(System.currentTimeMillis());
            snapshotCount.incrementAndGet();

            System.out.println("[Snapshot] Generated snapshot in " +
                (System.currentTimeMillis() - startTime) + "ms, size=" + snapshot.length + " bytes");

            return snapshot;
        } finally {
            inProgress.set(false);
        }
    }

    /**
     * Generates an empty RDB file for replicas connecting to an empty master.
     *
     * @return Empty RDB file bytes
     */
    public byte[] generateEmptySnapshot() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            // Header
            out.write(RDB_MAGIC);
            out.write(RDB_VERSION);

            // Auxiliary fields
            writeAuxField(out, "redis-ver", "7.0.0");
            writeAuxField(out, "redis-bits", "64");
            writeAuxField(out, "ctime", String.valueOf(System.currentTimeMillis() / 1000));
            writeAuxField(out, "used-mem", "0");

            // EOF
            out.write(RDB_OPCODE_EOF);

            // CRC64 checksum (zeros for now)
            out.write(new byte[8]);

            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate empty RDB", e);
        }
    }

    /**
     * Gets the RDB wrapped in RESP bulk string format for network transfer.
     *
     * @param rdb The RDB bytes
     * @return RESP-formatted transfer data
     */
    public byte[] wrapForTransfer(byte[] rdb) {
        String header = "$" + rdb.length + "\r\n";
        byte[] headerBytes = header.getBytes(StandardCharsets.US_ASCII);

        byte[] result = new byte[headerBytes.length + rdb.length];
        System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
        System.arraycopy(rdb, 0, result, headerBytes.length, rdb.length);

        return result;
    }

    // ==================== Internal RDB Generation ====================

    /**
     * Creates the actual RDB snapshot from database state.
     */
    private byte[] createRdbSnapshot() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            RedisDatabase db = RedisDatabase.getInstance();

            // Header
            out.write(RDB_MAGIC);
            out.write(RDB_VERSION);

            // Auxiliary fields
            writeAuxField(out, "redis-ver", "7.0.0");
            writeAuxField(out, "redis-bits", "64");
            writeAuxField(out, "ctime", String.valueOf(System.currentTimeMillis() / 1000));

            // Select database 0
            out.write(RDB_OPCODE_SELECTDB);
            writeLength(out, 0);

            // Write all key-value pairs
            Map<String, RedisValue> snapshot = db.getSnapshot();

            // Database size hint
            out.write(RDB_OPCODE_RESIZEDB);
            writeLength(out, snapshot.size()); // db size
            writeLength(out, 0); // expires size (simplified)

            for (Map.Entry<String, RedisValue> entry : snapshot.entrySet()) {
                String key = entry.getKey();
                RedisValue value = entry.getValue();

                // Skip if expired
                if (value.isExpired()) {
                    continue;
                }

                // Write expiry if set
                Long expiry = value.getExpiryTime();
                if (expiry != null && expiry > 0) {
                    out.write(RDB_OPCODE_EXPIRETIME_MS);
                    writeLongLE(out, expiry);
                }

                // Write value based on type
                switch (value.getType()) {
                    case STRING:
                        out.write(RDB_TYPE_STRING);
                        writeString(out, key);
                        writeString(out, (String) value.getData());
                        break;

                    case LIST:
                        out.write(RDB_TYPE_LIST);
                        writeString(out, key);
                        @SuppressWarnings("unchecked")
                        java.util.List<String> list = (java.util.List<String>) value.getData();
                        writeLength(out, list.size());
                        for (String item : list) {
                            writeString(out, item);
                        }
                        break;

                    case STREAM:
                        // Streams are complex; simplified for now
                        out.write(RDB_TYPE_STREAM);
                        writeString(out, key);
                        // Write empty stream marker
                        writeLength(out, 0);
                        break;

                    default:
                        // Skip unknown types
                        break;
                }
            }

            // EOF
            out.write(RDB_OPCODE_EOF);

            // CRC64 checksum (zeros - proper implementation would calculate this)
            out.write(new byte[8]);

            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create RDB snapshot", e);
        }
    }

    // ==================== RDB Encoding Helpers ====================

    private void writeAuxField(ByteArrayOutputStream out, String key, String value) throws IOException {
        out.write(RDB_OPCODE_AUX);
        writeString(out, key);
        writeString(out, value);
    }

    private void writeString(ByteArrayOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeLength(out, bytes.length);
        out.write(bytes);
    }

    private void writeLength(ByteArrayOutputStream out, int length) throws IOException {
        if (length < 64) {
            out.write(length);
        } else if (length < 16384) {
            out.write(0x40 | (length >> 8));
            out.write(length & 0xFF);
        } else {
            out.write(0x80);
            out.write((length >> 24) & 0xFF);
            out.write((length >> 16) & 0xFF);
            out.write((length >> 8) & 0xFF);
            out.write(length & 0xFF);
        }
    }

    private void writeLongLE(ByteArrayOutputStream out, long value) throws IOException {
        // Little-endian 8-byte integer
        for (int i = 0; i < 8; i++) {
            out.write((int) (value & 0xFF));
            value >>= 8;
        }
    }

    // ==================== State Accessors ====================

    public boolean isInProgress() {
        return inProgress.get();
    }

    public long getBaselineOffset() {
        return baselineOffset.get();
    }

    public long getLastSnapshotTime() {
        return lastSnapshotTime.get();
    }

    public long getSnapshotCount() {
        return snapshotCount.get();
    }

    // ==================== Reset (Testing) ====================

    public static void reset() {
        synchronized (INIT_LOCK) {
            INSTANCE = null;
        }
    }
}
