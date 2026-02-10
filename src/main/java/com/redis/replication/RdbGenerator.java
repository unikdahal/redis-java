package com.redis.replication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Generates RDB (Redis Database) files for replication.
 * <p>
 * RDB is Redis's binary serialization format for persisting and transferring
 * database snapshots. During full resynchronization, the master sends an RDB
 * file to the replica to establish the initial dataset.
 *
 * <h2>RDB File Format Structure</h2>
 * <pre>
 * ┌──────────────────────────────────────────────────────────────┐
 * │ Header                                                       │
 * │   ├── Magic: "REDIS" (5 bytes)                              │
 * │   └── Version: "0011" (4 bytes, ASCII)                      │
 * ├──────────────────────────────────────────────────────────────┤
 * │ Auxiliary Fields (Optional)                                  │
 * │   ├── FA redis-ver "7.0.0"                                  │
 * │   ├── FA redis-bits "64"                                    │
 * │   └── ...                                                   │
 * ├──────────────────────────────────────────────────────────────┤
 * │ Database Selection                                           │
 * │   └── FE 00 (select database 0)                             │
 * ├──────────────────────────────────────────────────────────────┤
 * │ Key-Value Pairs                                              │
 * │   └── [type][key][value] ...                                │
 * ├──────────────────────────────────────────────────────────────┤
 * │ EOF Marker                                                   │
 * │   └── FF                                                    │
 * ├──────────────────────────────────────────────────────────────┤
 * │ CRC64 Checksum (8 bytes)                                    │
 * └──────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Opcodes</h2>
 * <ul>
 *   <li>FA (0xFA): Auxiliary field</li>
 *   <li>FE (0xFE): Database selector</li>
 *   <li>FB (0xFB): Resize database (hash table sizes)</li>
 *   <li>FF (0xFF): End of file</li>
 * </ul>
 *
 * <h2>Length Encoding</h2>
 * <ul>
 *   <li>00xxxxxx: 6-bit length (0-63)</li>
 *   <li>01xxxxxx xxxxxxxx: 14-bit length (64-16383)</li>
 *   <li>10000000 xxxxxxxx×4: 32-bit length (big endian)</li>
 * </ul>
 *
 * <h2>Current Limitations</h2>
 * <ul>
 *   <li>Only generates empty RDB files (no data serialization)</li>
 *   <li>CRC64 checksum is set to zeros (not calculated)</li>
 *   <li>Full database serialization not yet implemented</li>
 * </ul>
 *
 * @see <a href="https://rdb.fnordig.de/file_format.html">RDB File Format</a>
 */
public class RdbGenerator {

    // ==================== RDB Opcodes ====================

    /** Auxiliary field marker */
    private static final byte RDB_OPCODE_AUX = (byte) 0xFA;

    /** Database selector */
    private static final byte RDB_OPCODE_SELECTDB = (byte) 0xFE;

    /** Hash table resize hint */
    private static final byte RDB_OPCODE_RESIZEDB = (byte) 0xFB;

    /** End of file marker */
    private static final byte RDB_OPCODE_EOF = (byte) 0xFF;

    // ==================== RDB Configuration ====================

    /** RDB format version (corresponds to Redis 7.0) */
    private static final String RDB_VERSION = "0011";

    /** Magic string that identifies RDB files */
    private static final String RDB_MAGIC = "REDIS";

    // ==================== Public API ====================

    /**
     * Produce a minimal valid RDB file suitable for a full resynchronization when the server has no data.
     *
     * The RDB includes the magic header, version, two auxiliary fields (`redis-ver` and `redis-bits`), an EOF marker,
     * and an 8-byte zero CRC64 placeholder.
     *
     * @return a byte array containing the generated RDB file
     */
    public static byte[] generateEmptyRdb() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            // --- Header ---
            // Magic string "REDIS"
            out.write(RDB_MAGIC.getBytes());

            // RDB version (4 ASCII characters)
            out.write(RDB_VERSION.getBytes());

            // --- Auxiliary Fields ---
            // redis-ver: Version identification
            out.write(RDB_OPCODE_AUX);
            writeString(out, "redis-ver");
            writeString(out, "7.0.0");

            // redis-bits: Architecture (32 or 64 bit)
            out.write(RDB_OPCODE_AUX);
            writeString(out, "redis-bits");
            writeString(out, "64");

            // --- EOF ---
            out.write(RDB_OPCODE_EOF);

            // --- CRC64 Checksum ---
            // 8 bytes of zeros (checksum not calculated)
            // In production, this should be a proper CRC64 of the file content
            out.write(new byte[8]);

            return out.toByteArray();

        } catch (IOException e) {
            // Should never happen with ByteArrayOutputStream
            throw new RuntimeException("Failed to generate RDB", e);
        }
    }

    /**
     * Wraps the generated RDB bytes with a RESP bulk-string header for transfer.
     *
     * <p>Format: `$&lt;length&gt;\r\n&lt;rdb-bytes&gt;` (no trailing `\r\n` after the RDB data).
     *
     * @return the RESP bulk-string representation: a literal-length header (`$<length>\r\n`)
     *         followed immediately by the RDB bytes, without a trailing CRLF
     */
    public static byte[] getRdbTransferFormat() {
        byte[] rdb = generateEmptyRdb();
        String header = "$" + rdb.length + "\r\n";

        byte[] headerBytes = header.getBytes();
        byte[] result = new byte[headerBytes.length + rdb.length];

        System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
        System.arraycopy(rdb, 0, result, headerBytes.length, rdb.length);

        return result;
    }

    // ==================== Internal Helpers ====================

    /**
     * Writes a string to the output stream prefixed by its length using RDB variable-length encoding.
     *
     * @param out the output stream to write to
     * @param s the string to write
     * @throws IOException if an I/O error occurs while writing
     */
    private static void writeString(ByteArrayOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes();
        writeLength(out, bytes.length);
        out.write(bytes);
    }

    /**
     * Writes a length value using RDB's variable-length encoding.
     * <p>
     * Encoding scheme:
     * <ul>
     *   <li>0-63: Single byte (00xxxxxx)</li>
     *   <li>64-16383: Two bytes (01xxxxxx xxxxxxxx)</li>
     *   <li>16384+: Five bytes (10000000 + 4 big-endian bytes)</li>
     * </ul>
     *
     * @param out Output stream
     * @param length Length to encode
     * @throws IOException If write fails
     */
    private static void writeLength(ByteArrayOutputStream out, int length) throws IOException {
        if (length < 64) {
            // 6-bit length: 00xxxxxx
            out.write(length);
        } else if (length < 16384) {
            // 14-bit length: 01xxxxxx xxxxxxxx
            out.write(0x40 | (length >> 8));
            out.write(length & 0xFF);
        } else {
            // 32-bit length: 10000000 + 4 bytes big-endian
            out.write(0x80);
            out.write((length >> 24) & 0xFF);
            out.write((length >> 16) & 0xFF);
            out.write((length >> 8) & 0xFF);
            out.write(length & 0xFF);
        }
    }
}