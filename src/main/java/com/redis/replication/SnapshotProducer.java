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

    /**
     * Initializes a new SnapshotProducer instance by creating and setting default atomic state fields.
     *
     * The following fields are initialized:
     * - inProgress: false
     * - baselineOffset: 0
     * - lastSnapshotTime: 0
     * - snapshotCount: 0
     */
    private SnapshotProducer() {
        this.inProgress = new AtomicBoolean(false);
        this.baselineOffset = new AtomicLong(0);
        this.lastSnapshotTime = new AtomicLong(0);
        this.snapshotCount = new AtomicLong(0);
    }

    /**
     * Retrieve the singleton SnapshotProducer instance, creating it lazily in a thread-safe manner.
     *
     * @return the singleton SnapshotProducer instance
     */
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
     * Create a minimal RDB file representing an empty dataset.
     *
     * @return the bytes of a minimal RDB file suitable for replica synchronization (contains header, auxiliary fields, EOF opcode, and an 8-byte CRC64 placeholder)
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
     * Builds a Redis RDB-format snapshot representing the current in-memory database state.
     *
     * The returned byte array is a complete RDB file containing header and auxiliary fields,
     * a SELECTDB opcode for database 0, a database size hint, serialized key-value entries
     * (including expiry timestamps when present), an EOF opcode, and an 8-byte CRC64 placeholder.
     *
     * @return a byte array containing the serialized RDB snapshot
     * @throws RuntimeException if snapshot serialization fails
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

    /**
     * Writes an AUX field entry to the RDB output stream: emits the AUX opcode followed by the
     * provided key and value encoded as RDB strings.
     *
     * @param out   the output stream to write the AUX entry to
     * @param key   the auxiliary field name
     * @param value the auxiliary field value
     * @throws IOException if an I/O error occurs while writing to the stream
     */

    private void writeAuxField(ByteArrayOutputStream out, String key, String value) throws IOException {
        out.write(RDB_OPCODE_AUX);
        writeString(out, key);
        writeString(out, value);
    }

    /**
     * Writes a UTF-8 encoded string to the given output stream, preceded by its Redis RDB length encoding.
     *
     * @param out the target ByteArrayOutputStream to write the length prefix and UTF-8 bytes into
     * @param s   the string to encode and write
     * @throws IOException if an I/O error occurs while writing to the stream
     */
    private void writeString(ByteArrayOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeLength(out, bytes.length);
        out.write(bytes);
    }

    /**
     * Encodes an integer using Redis RDB length encoding and writes the resulting bytes to the output stream.
     *
     * <p>Encoding forms:
     * <ul>
     *   <li>0 <= length &lt; 64: single byte containing the length.</li>
     *   <li>64 <= length &lt; 16384: two bytes with 0x40 prefix in the first byte followed by the low 8 bits.</li>
     *   <li>length >= 16384: marker byte 0x80 followed by a 4-byte big-endian length.</li>
     * </ul>
     *
     * @param out the output stream to write encoded length bytes to
     * @param length the integer length to encode
     * @throws IOException if an I/O error occurs while writing to the stream
     */
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

    /**
     * Writes the given long as an 8-byte little-endian integer to the provided output stream.
     *
     * @param out   the stream to write the bytes to
     * @param value the long value to encode
     * @throws IOException if an I/O error occurs while writing to the stream
     */
    private void writeLongLE(ByteArrayOutputStream out, long value) throws IOException {
        // Little-endian 8-byte integer
        for (int i = 0; i < 8; i++) {
            out.write((int) (value & 0xFF));
            value >>= 8;
        }
    }

    /**
     * Indicates whether a snapshot is currently being produced.
     *
     * @return `true` if a snapshot is in progress, `false` otherwise.
     */

    public boolean isInProgress() {
        return inProgress.get();
    }

    /**
     * Replication offset recorded when the most recent snapshot generation began.
     *
     * @return the baseline replication offset captured at snapshot start
     */
    public long getBaselineOffset() {
        return baselineOffset.get();
    }

    /**
     * Returns the timestamp when the most recent snapshot was produced.
     *
     * @return the last snapshot time in milliseconds since the Unix epoch, or 0 if no snapshot has been produced yet
     */
    public long getLastSnapshotTime() {
        return lastSnapshotTime.get();
    }

    /**
     * The total number of snapshots produced by this SnapshotProducer.
     *
     * @return the total number of snapshots produced
     */
    public long getSnapshotCount() {
        return snapshotCount.get();
    }

    /**
     * Reset the SnapshotProducer singleton, clearing the cached instance so a new instance will be created on the next call to getInstance().
     *
     * This operation acquires the initialization lock to perform the reset in a thread-safe manner (intended for use in tests).
     */

    public static void reset() {
        synchronized (INIT_LOCK) {
            INSTANCE = null;
        }
    }
}