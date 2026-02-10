package com.redis.replication;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the connection from a replica (slave) to its master server.
 * <p>
 * This class handles the complete replication lifecycle:
 * <ol>
 *   <li>TCP connection establishment</li>
 *   <li>Replication handshake (PING → REPLCONF → PSYNC)</li>
 *   <li>RDB file reception for full sync</li>
 *   <li>Command stream processing</li>
 *   <li>ACK responses for WAIT synchronization</li>
 * </ol>
 *
 * <h2>Handshake Protocol</h2>
 * <pre>
 *  Replica                         Master
 *     │                               │
 *     │──────── PING ────────────────►│
 *     │◄─────── +PONG ────────────────│
 *     │                               │
 *     │── REPLCONF listening-port ───►│
 *     │◄─────── +OK ──────────────────│
 *     │                               │
 *     │── REPLCONF capa psync2 ──────►│
 *     │◄─────── +OK ──────────────────│
 *     │                               │
 *     │── PSYNC ? -1 ────────────────►│
 *     │◄─ +FULLRESYNC &lt;id&gt; &lt;offset&gt; ─│
 *     │◄─ $&lt;len&gt;\r\n&lt;RDB&gt; ────────────│
 *     │                               │
 *     │◄─ (streaming commands) ───────│
 *     │                               │
 *     │◄─ REPLCONF GETACK * ──────────│
 *     │── REPLCONF ACK &lt;offset&gt; ─────►│
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * <ul>
 *   <li>All Netty I/O runs on a dedicated EventLoopGroup</li>
 *   <li>State variables are volatile for visibility</li>
 *   <li>Offset tracking uses AtomicLong</li>
 * </ul>
 *
 * <h2>Error Handling</h2>
 * <ul>
 *   <li>Connection failures logged with context</li>
 *   <li>Parse errors handled gracefully</li>
 *   <li>Disconnection triggers cleanup</li>
 * </ul>
 *
 * @see ReplicationManager
 * @see HandshakeState
 */
public class MasterConnection {

    // ==================== Connection Configuration ====================

    /** Master server hostname or IP address */
    private final String masterHost;

    /** Master server port */
    private final int masterPort;

    /** This replica's listening port (reported to master) */
    private final int listeningPort;

    // ==================== Connection State ====================

    /** Netty channel to master */
    private Channel channel;

    /** Dedicated event loop for master connection */
    private EventLoopGroup workerGroup;

    /** Connection status flag */
    private volatile boolean connected;

    /** Current position in handshake state machine */
    private volatile HandshakeState handshakeState;

    // ==================== Replication Tracking ====================

    /**
     * Total bytes processed from the replication stream.
     * Used to respond to REPLCONF GETACK requests.
     */
    private final AtomicLong bytesProcessed;

    // ==================== RDB Loading State ====================

    /** Flag indicating we're currently receiving RDB data */
    private volatile boolean loadingRdb;

    /** Buffer for accumulating RDB file bytes */
    private ByteBuf rdbBuffer;

    /** Expected size of RDB file in bytes */
    private int expectedRdbSize;

    // ==================== State Enum ====================

    /**
     * States in the replication handshake and streaming lifecycle.
     */
    public enum HandshakeState {
        /** Connection not yet initiated */
        NOT_STARTED,

        /** PING sent, awaiting PONG */
        PING_SENT,

        /** REPLCONF listening-port sent */
        REPLCONF_PORT_SENT,

        /** REPLCONF capa sent */
        REPLCONF_CAPA_SENT,

        /** PSYNC sent, awaiting FULLRESYNC or CONTINUE */
        PSYNC_SENT,

        /** Receiving RDB file data */
        RDB_LOADING,

        /** Normal command streaming mode */
        STREAMING
    }

    // ==================== Constructor ====================

    /**
     * Creates a new master connection handler.
     *
     * @param masterHost Master's hostname or IP
     * @param masterPort Master's port
     * @param listeningPort This replica's listening port
     */
    public MasterConnection(String masterHost, int masterPort, int listeningPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.listeningPort = listeningPort;
        this.handshakeState = HandshakeState.NOT_STARTED;
        this.bytesProcessed = new AtomicLong(0);
        this.connected = false;
        this.loadingRdb = false;
    }

    // ==================== Connection Management ====================

    /**
     * Initiates asynchronous connection to the master server.
     *
     * <p>Upon successful connection, automatically begins the
     * replication handshake sequence.
     *
     * @return CompletableFuture that completes when connection is established
     */
    public CompletableFuture<Void> connect() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        // Create dedicated event loop for this connection
        workerGroup = new NioEventLoopGroup(1);

        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)  // Disable Nagle for low latency
         .option(ChannelOption.SO_KEEPALIVE, true) // Enable TCP keepalive
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new MasterResponseHandler());
             }
         });

        // Initiate async connection
        b.connect(masterHost, masterPort).addListener((ChannelFuture cf) -> {
            if (cf.isSuccess()) {
                channel = cf.channel();
                connected = true;
                System.out.println("[Replication] Connected to master at " + masterHost + ":" + masterPort);

                // Begin handshake
                startHandshake();
                future.complete(null);
            } else {
                System.err.println("[Replication] Failed to connect to master: " + cf.cause().getMessage());
                future.completeExceptionally(cf.cause());
            }
        });

        return future;
    }

    /**
     * Initiates the replication handshake with PING.
     */
    private void startHandshake() {
        handshakeState = HandshakeState.PING_SENT;
        sendCommand("PING");
    }

    /**
     * Sends a RESP-formatted command to the master.
     * Uses UTF-8 byte length for proper RESP encoding.
     *
     * @param args Command name followed by arguments
     */
    private void sendCommand(String... args) {
        // Build RESP array header
        StringBuilder header = new StringBuilder();
        header.append("*").append(args.length).append("\r\n");

        // Pre-compute byte arrays for each argument
        byte[][] argBytes = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            argBytes[i] = args[i].getBytes(StandardCharsets.UTF_8);
        }

        // Calculate total buffer size
        int totalSize = header.length();
        for (byte[] argByte : argBytes) {
            // $<len>\r\n<data>\r\n
            totalSize += 1 + String.valueOf(argByte.length).length() + 2 + argByte.length + 2;
        }

        ByteBuf buf = Unpooled.buffer(totalSize);
        buf.writeBytes(header.toString().getBytes(StandardCharsets.UTF_8));

        for (byte[] argByte : argBytes) {
            buf.writeByte('$');
            buf.writeBytes(String.valueOf(argByte.length).getBytes(StandardCharsets.UTF_8));
            buf.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
            buf.writeBytes(argByte);
            buf.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        }

        channel.writeAndFlush(buf);
    }

    // ==================== Response Handler ====================

    /**
     * Netty handler for processing responses from the master.
     * <p>
     * Handles both RESP protocol parsing and RDB file reception.
     */
    private class MasterResponseHandler extends ByteToMessageDecoder {

        /** Buffer for parsing RESP array elements */
        private final List<String> argsBuffer = new ArrayList<>();

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            while (in.readableBytes() > 0) {
                // Special handling for RDB data - do NOT mark/reset here
                // as RDB bytes are consumed progressively by handleRdbData
                if (loadingRdb) {
                    if (!handleRdbData(ctx, in)) {
                        // Not enough data yet, wait for more
                        return;
                    }
                    continue;
                }

                // Mark before RESP parsing - only reset if RESP parse is incomplete
                in.markReaderIndex();
                int startReaderIndex = in.readerIndex();

                // Parse RESP response
                argsBuffer.clear();
                if (!parseRespResponse(in, argsBuffer)) {
                    in.resetReaderIndex();
                    return;
                }

                // Calculate bytes consumed by this RESP message for accurate REPLCONF ACK
                int bytesConsumed = in.readerIndex() - startReaderIndex;

                // Only track bytes during streaming mode (not during handshake)
                if (handshakeState == HandshakeState.STREAMING) {
                    bytesProcessed.addAndGet(bytesConsumed);
                }

                // Route response based on current handshake state
                handleResponse(argsBuffer);
            }
        }

        // ==================== RESP Parsing ====================

        /**
         * Parses a single RESP value from the buffer.
         *
         * @param buf Input buffer
         * @param result Output list for parsed values
         * @return true if complete value was parsed, false if more data needed
         */
        private boolean parseRespResponse(ByteBuf buf, List<String> result) {
            if (buf.readableBytes() < 1) return false;

            byte type = buf.readByte();

            return switch (type) {
                case '+' -> parseSimpleString(buf, result);  // Simple String
                case '-' -> parseSimpleString(buf, result);  // Error
                case ':' -> parseSimpleString(buf, result);  // Integer
                case '$' -> parseBulkString(buf, result);    // Bulk String
                case '*' -> parseArray(buf, result);         // Array
                default -> {
                    // Try inline command format
                    buf.readerIndex(buf.readerIndex() - 1);
                    yield parseInlineCommand(buf, result);
                }
            };
        }

        /**
         * Parses a simple string (or error/integer) ending in CRLF.
         */
        private boolean parseSimpleString(ByteBuf buf, List<String> result) {
            int start = buf.readerIndex();
            int end = buf.indexOf(start, buf.writerIndex(), (byte) '\r');
            if (end == -1 || buf.readableBytes() < (end - start + 2)) return false;

            CharSequence value = buf.readCharSequence(end - start, StandardCharsets.UTF_8);
            buf.skipBytes(2); // Skip \r\n
            result.add(value.toString());
            return true;
        }

        /**
         * Parses a bulk string: $<length>\r\n<data>\r\n
         */
        private boolean parseBulkString(ByteBuf buf, List<String> result) {
            int startIndex = buf.readerIndex();
            int end = buf.indexOf(startIndex, buf.writerIndex(), (byte) '\r');
            if (end == -1 || buf.readableBytes() < (end - startIndex + 2)) return false;

            CharSequence lenStr = buf.readCharSequence(end - startIndex, StandardCharsets.UTF_8);
            buf.skipBytes(2); // Skip \r\n

            int len;
            try {
                len = Integer.parseInt(lenStr.toString());
            } catch (NumberFormatException e) {
                System.err.println("[Replication] Malformed bulk string length: '" + lenStr + "'");
                buf.readerIndex(startIndex);
                return false;
            }

            // Handle null bulk string
            if (len == -1) {
                result.add(null);
                return true;
            }

            // Validate length is not negative (except -1 sentinel)
            if (len < 0) {
                System.err.println("[Replication] Invalid bulk string length: " + len);
                buf.readerIndex(startIndex);
                return false;
            }

            if (buf.readableBytes() < len + 2) {
                buf.readerIndex(startIndex);
                return false;
            }

            CharSequence value = buf.readCharSequence(len, StandardCharsets.UTF_8);
            buf.skipBytes(2); // Skip \r\n
            result.add(value.toString());
            return true;
        }

        /**
         * Parses an array: *<count>\r\n followed by elements.
         */
        private boolean parseArray(ByteBuf buf, List<String> result) {
            int startIndex = buf.readerIndex();
            int end = buf.indexOf(startIndex, buf.writerIndex(), (byte) '\r');
            if (end == -1 || buf.readableBytes() < (end - startIndex + 2)) return false;

            CharSequence lenStr = buf.readCharSequence(end - startIndex, StandardCharsets.UTF_8);
            buf.skipBytes(2); // Skip \r\n

            int numElements;
            try {
                numElements = Integer.parseInt(lenStr.toString());
            } catch (NumberFormatException e) {
                System.err.println("[Replication] Malformed array count: '" + lenStr + "'");
                buf.readerIndex(startIndex);
                return false;
            }

            // Handle null array
            if (numElements == -1) {
                result.add(null);
                return true;
            }

            // Validate element count is not negative (except -1 sentinel)
            if (numElements < 0) {
                System.err.println("[Replication] Invalid array element count: " + numElements);
                buf.readerIndex(startIndex);
                return false;
            }

            for (int i = 0; i < numElements; i++) {
                List<String> element = new ArrayList<>();
                if (!parseRespResponse(buf, element)) {
                    buf.readerIndex(startIndex);
                    return false;
                }
                result.addAll(element);
            }

            return true;
        }

        /**
         * Parses an inline command (space-separated).
         */
        private boolean parseInlineCommand(ByteBuf buf, List<String> result) {
            int start = buf.readerIndex();
            int end = buf.indexOf(start, buf.writerIndex(), (byte) '\r');
            if (end == -1 || buf.readableBytes() < (end - start + 2)) return false;

            CharSequence line = buf.readCharSequence(end - start, StandardCharsets.UTF_8);
            buf.skipBytes(2); // Skip \r\n

            String[] parts = line.toString().split(" ");
            for (String part : parts) {
                result.add(part);
            }
            return true;
        }

        // ==================== Response Routing ====================

        /**
         * Routes parsed response based on current handshake state.
         */
        private void handleResponse(List<String> response) {
            if (response.isEmpty()) return;

            String firstArg = response.get(0);

            switch (handshakeState) {
                case PING_SENT -> handlePingResponse(firstArg);
                case REPLCONF_PORT_SENT -> handleReplconfPortResponse(firstArg);
                case REPLCONF_CAPA_SENT -> handleReplconfCapaResponse(firstArg);
                case PSYNC_SENT -> handlePsyncResponse(firstArg);
                case STREAMING -> handleStreamingCommand(response);
            }
        }

        private void handlePingResponse(String response) {
            if ("PONG".equalsIgnoreCase(response)) {
                System.out.println("[Replication] Master responded to PING");
                handshakeState = HandshakeState.REPLCONF_PORT_SENT;
                sendCommand("REPLCONF", "listening-port", String.valueOf(listeningPort));
            }
        }

        private void handleReplconfPortResponse(String response) {
            if ("OK".equalsIgnoreCase(response)) {
                System.out.println("[Replication] REPLCONF listening-port acknowledged");
                handshakeState = HandshakeState.REPLCONF_CAPA_SENT;
                sendCommand("REPLCONF", "capa", "psync2");
            }
        }

        private void handleReplconfCapaResponse(String response) {
            if ("OK".equalsIgnoreCase(response)) {
                System.out.println("[Replication] REPLCONF capa acknowledged");
                handshakeState = HandshakeState.PSYNC_SENT;
                sendCommand("PSYNC", "?", "-1");
            }
        }

        private void handlePsyncResponse(String response) {
            if (response.startsWith("FULLRESYNC")) {
                // Parse: FULLRESYNC <replid> <offset>
                String[] parts = response.split(" ");
                if (parts.length >= 3) {
                    String replId = parts[1];
                    long offset;
                    try {
                        offset = Long.parseLong(parts[2]);
                    } catch (NumberFormatException e) {
                        System.err.println("[Replication] Malformed FULLRESYNC offset: '" + parts[2] +
                            "' in response: " + response);
                        // Do not change state - bail out without disrupting the pipeline
                        return;
                    }
                    System.out.println("[Replication] Full resync: replId=" + replId + ", offset=" + offset);

                    ReplicationManager.getInstance().setMasterReplOffset(offset);
                    handshakeState = HandshakeState.RDB_LOADING;
                    loadingRdb = true;
                    expectedRdbSize = -1;
                }
            } else if (response.startsWith("CONTINUE")) {
                System.out.println("[Replication] Partial resync - continuing");
                handshakeState = HandshakeState.STREAMING;
                loadingRdb = false;
            }
        }

        // ==================== RDB Handling ====================

        /**
         * Handles incoming RDB file data.
         *
         * @param ctx Channel context for error handling
         * @param in Input buffer
         * @return true if RDB is complete, false if more data needed
         */
        private boolean handleRdbData(ChannelHandlerContext ctx, ByteBuf in) {
            // First, read the RDB size if not yet known
            if (expectedRdbSize == -1) {
                if (in.readableBytes() < 1) return false;

                // Peek at marker without consuming
                byte marker = in.getByte(in.readerIndex());
                if (marker != '$') {
                    System.err.println("[Replication] Protocol error: Expected RDB size marker '$', got: " +
                        (char) marker + " (0x" + Integer.toHexString(marker & 0xFF) + ")");
                    // Cleanup RDB state and close connection
                    cleanupRdbState();
                    ctx.close();
                    return false;
                }
                // Now consume the marker
                in.readByte();

                int start = in.readerIndex();
                int end = in.indexOf(start, in.writerIndex(), (byte) '\r');
                if (end == -1) {
                    in.readerIndex(in.readerIndex() - 1);
                    return false;
                }

                CharSequence sizeStr = in.readCharSequence(end - start, StandardCharsets.UTF_8);
                in.skipBytes(2); // Skip \r\n

                try {
                    expectedRdbSize = Integer.parseInt(sizeStr.toString());
                } catch (NumberFormatException e) {
                    System.err.println("[Replication] Malformed RDB size: '" + sizeStr + "'");
                    // Revert reader index adjustments
                    in.readerIndex(start - 1); // Back to before '$' marker
                    cleanupRdbState();
                    ctx.close();
                    return false;
                }

                if (expectedRdbSize < 0) {
                    System.err.println("[Replication] Invalid RDB size: " + expectedRdbSize);
                    cleanupRdbState();
                    ctx.close();
                    return false;
                }

                System.out.println("[Replication] Expecting RDB file: " + expectedRdbSize + " bytes");
                rdbBuffer = Unpooled.buffer(expectedRdbSize);
            }

            // Read RDB data
            int remaining = expectedRdbSize - rdbBuffer.readableBytes();
            int toRead = Math.min(remaining, in.readableBytes());

            in.readBytes(rdbBuffer, toRead);

            if (rdbBuffer.readableBytes() >= expectedRdbSize) {
                // RDB complete
                System.out.println("[Replication] RDB file received (" + expectedRdbSize + " bytes)");
                processRdbFile(rdbBuffer);
                rdbBuffer.release();
                rdbBuffer = null;

                loadingRdb = false;
                handshakeState = HandshakeState.STREAMING;
                System.out.println("[Replication] Now streaming from master");
                return true;
            }

            return false; // Need more data
        }

        /**
         * Processes the received RDB file.
         * <p>
         * For now, just acknowledges receipt. A full implementation
         * would parse the RDB format and restore database state.
         */
        private void processRdbFile(ByteBuf rdb) {
            System.out.println("[Replication] RDB processing complete (empty database mode)");
        }

        // ==================== Streaming Command Handling ====================

        /**
         * Handles replicated commands from the master.
         */
        private void handleStreamingCommand(List<String> command) {
            if (command.isEmpty()) return;

            String cmdName = command.get(0).toUpperCase();

            // Handle REPLCONF GETACK - respond with our current offset
            if ("REPLCONF".equals(cmdName) && command.size() >= 2
                && "GETACK".equalsIgnoreCase(command.get(1))) {
                long offset = bytesProcessed.get();
                sendCommand("REPLCONF", "ACK", String.valueOf(offset));
                return;
            }

            // Execute the replicated command locally
            executeReplicatedCommand(command);
        }

        /**
         * Executes a replicated write command on the local database.
         * <p>
         * Handles both original and canonicalized command forms from master.
         * Canonicalized forms include:
         * <ul>
         *   <li>SET key value PXAT timestamp (absolute expiry)</li>
         *   <li>PEXPIREAT key timestamp</li>
         *   <li>LPOP key (from BLPOP)</li>
         * </ul>
         */
        private void executeReplicatedCommand(List<String> command) {
            if (command.isEmpty()) return;

            String cmdName = command.get(0).toUpperCase();
            RedisDatabase db = RedisDatabase.getInstance();

            try {
                switch (cmdName) {
                    case "SET" -> handleSetCommand(command, db);
                    case "DEL" -> handleDelCommand(command, db);
                    case "INCR" -> handleIncrCommand(command, db);
                    case "LPUSH" -> handleLPushCommand(command, db);
                    case "RPUSH" -> handleRPushCommand(command, db);
                    case "LPOP" -> handleLPopCommand(command, db);
                    case "RPOP" -> handleRPopCommand(command, db);
                    case "EXPIRE" -> handleExpireCommand(command, db);
                    case "PEXPIRE" -> handlePExpireCommand(command, db);
                    case "EXPIREAT" -> handleExpireAtCommand(command, db);
                    case "PEXPIREAT" -> handlePExpireAtCommand(command, db);
                    case "XADD" -> handleXAddCommand(command, db);
                    case "HSET" -> handleHSetCommand(command, db);
                    case "SADD" -> handleSAddCommand(command, db);
                    case "ZADD" -> handleZAddCommand(command, db);
                    default -> {
                        // Log unrecognized commands for debugging divergence
                        System.err.println("[Replication] Unrecognized replicated command: " + cmdName +
                            " (args: " + command.subList(1, Math.min(command.size(), 4)) + ")");
                    }
                }
            } catch (Exception e) {
                System.err.println("[Replication] Error executing replicated command " + cmdName + ": " + e.getMessage());
            }
        }

        private void handleSetCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 3) return;
            String key = command.get(1);
            String value = command.get(2);

            // Handle canonicalized PXAT form (absolute millisecond timestamp)
            if (command.size() >= 5 && "PXAT".equalsIgnoreCase(command.get(3))) {
                try {
                    long absTimeMs = Long.parseLong(command.get(4));
                    long ttlMs = absTimeMs - System.currentTimeMillis();
                    if (ttlMs > 0) {
                        db.put(key, value, ttlMs);
                    } else {
                        // Already expired, don't store
                        db.remove(key);
                    }
                } catch (NumberFormatException e) {
                    db.put(key, value);
                }
            } else if (command.size() >= 5 && "PX".equalsIgnoreCase(command.get(3))) {
                try {
                    long px = Long.parseLong(command.get(4));
                    db.put(key, value, px);
                } catch (NumberFormatException e) {
                    db.put(key, value);
                }
            } else if (command.size() >= 5 && "EX".equalsIgnoreCase(command.get(3))) {
                try {
                    long ex = Long.parseLong(command.get(4)) * 1000;
                    db.put(key, value, ex);
                } catch (NumberFormatException e) {
                    db.put(key, value);
                }
            } else if (command.size() >= 5 && "EXAT".equalsIgnoreCase(command.get(3))) {
                try {
                    long absTimeSec = Long.parseLong(command.get(4));
                    long ttlMs = (absTimeSec * 1000) - System.currentTimeMillis();
                    if (ttlMs > 0) {
                        db.put(key, value, ttlMs);
                    } else {
                        db.remove(key);
                    }
                } catch (NumberFormatException e) {
                    db.put(key, value);
                }
            } else {
                db.put(key, value);
            }
        }

        private void handleDelCommand(List<String> command, RedisDatabase db) {
            for (int i = 1; i < command.size(); i++) {
                db.remove(command.get(i));
            }
        }

        private void handleIncrCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 2) return;
            String key = command.get(1);
            String current = db.get(key);
            try {
                long val = (current == null || current.isEmpty()) ? 0 : Long.parseLong(current);
                db.put(key, String.valueOf(val + 1));
            } catch (NumberFormatException e) {
                // Non-numeric value: Redis would error, but for replication consistency
                // we log and skip (master already validated)
                System.err.println("[Replication] INCR on non-numeric value for key: " + key);
            }
        }

        private void handleLPushCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 3) return;
            String key = command.get(1);
            db.compute(key, existing -> {
                java.util.LinkedList<String> list;
                if (existing == null) {
                    list = new java.util.LinkedList<>();
                } else if (existing.getType() != RedisValue.Type.LIST) {
                    return existing;
                } else {
                    @SuppressWarnings("unchecked")
                    java.util.List<String> existingList = (java.util.List<String>) existing.getData();
                    list = new java.util.LinkedList<>(existingList);
                }
                for (int i = 2; i < command.size(); i++) {
                    list.addFirst(command.get(i));
                }
                return RedisValue.list(list);
            });
        }

        private void handleRPushCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 3) return;
            String key = command.get(1);
            db.compute(key, existing -> {
                java.util.LinkedList<String> list;
                if (existing == null) {
                    list = new java.util.LinkedList<>();
                } else if (existing.getType() != RedisValue.Type.LIST) {
                    return existing;
                } else {
                    @SuppressWarnings("unchecked")
                    java.util.List<String> existingList = (java.util.List<String>) existing.getData();
                    list = new java.util.LinkedList<>(existingList);
                }
                for (int i = 2; i < command.size(); i++) {
                    list.addLast(command.get(i));
                }
                return RedisValue.list(list);
            });
        }

        private void handleLPopCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 2) return;
            String key = command.get(1);
            db.compute(key, existing -> {
                if (existing == null || existing.getType() != RedisValue.Type.LIST) {
                    return existing;
                }
                @SuppressWarnings("unchecked")
                java.util.List<String> existingList = (java.util.List<String>) existing.getData();
                if (existingList.isEmpty()) {
                    return existing;
                }
                java.util.LinkedList<String> list = new java.util.LinkedList<>(existingList);
                list.removeFirst();
                return list.isEmpty() ? null : RedisValue.list(list);
            });
        }

        private void handleRPopCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 2) return;
            String key = command.get(1);
            db.compute(key, existing -> {
                if (existing == null || existing.getType() != RedisValue.Type.LIST) {
                    return existing;
                }
                @SuppressWarnings("unchecked")
                java.util.List<String> existingList = (java.util.List<String>) existing.getData();
                if (existingList.isEmpty()) {
                    return existing;
                }
                java.util.LinkedList<String> list = new java.util.LinkedList<>(existingList);
                list.removeLast();
                return list.isEmpty() ? null : RedisValue.list(list);
            });
        }

        private void handleExpireCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 3) return;
            try {
                String key = command.get(1);
                long seconds = Long.parseLong(command.get(2));
                db.setExpiryTime(key, System.currentTimeMillis() + (seconds * 1000));
            } catch (NumberFormatException ignored) {}
        }

        private void handlePExpireCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 3) return;
            try {
                String key = command.get(1);
                long ms = Long.parseLong(command.get(2));
                db.setExpiryTime(key, System.currentTimeMillis() + ms);
            } catch (NumberFormatException ignored) {}
        }

        private void handleExpireAtCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 3) return;
            try {
                String key = command.get(1);
                long absTimeSec = Long.parseLong(command.get(2));
                db.setExpiryTime(key, absTimeSec * 1000);
            } catch (NumberFormatException ignored) {}
        }

        private void handlePExpireAtCommand(List<String> command, RedisDatabase db) {
            if (command.size() < 3) return;
            try {
                String key = command.get(1);
                long absTimeMs = Long.parseLong(command.get(2));
                db.setExpiryTime(key, absTimeMs);
            } catch (NumberFormatException ignored) {}
        }

        private void handleXAddCommand(List<String> command, RedisDatabase db) {
            // XADD stream id field value [field value ...]
            if (command.size() < 5) return;
            String streamKey = command.get(1);
            String idStr = command.get(2);

            // Parse stream ID
            com.redis.util.StreamId streamId;
            try {
                streamId = com.redis.util.StreamId.parse(idStr);
            } catch (Exception e) {
                System.err.println("[Replication] Invalid stream ID in XADD: " + idStr);
                return;
            }

            // Collect field-value pairs
            java.util.Map<String, String> fields = new java.util.LinkedHashMap<>();
            for (int i = 3; i + 1 < command.size(); i += 2) {
                fields.put(command.get(i), command.get(i + 1));
            }

            db.compute(streamKey, existing -> {
                java.util.Map<com.redis.util.StreamId, java.util.Map<String, String>> stream;
                if (existing == null) {
                    stream = new java.util.concurrent.ConcurrentSkipListMap<>();
                } else if (existing.getType() != RedisValue.Type.STREAM) {
                    return existing;
                } else {
                    @SuppressWarnings("unchecked")
                    java.util.Map<com.redis.util.StreamId, java.util.Map<String, String>> existingStream =
                        (java.util.Map<com.redis.util.StreamId, java.util.Map<String, String>>) existing.getData();
                    stream = new java.util.concurrent.ConcurrentSkipListMap<>(existingStream);
                }

                stream.put(streamId, fields);

                return RedisValue.stream(stream);
            });
        }

        private void handleHSetCommand(List<String> command, RedisDatabase db) {
            // HSET key field value [field value ...]
            if (command.size() < 4) return;
            String key = command.get(1);

            db.compute(key, existing -> {
                java.util.Map<String, String> hash;
                if (existing == null) {
                    hash = new java.util.HashMap<>();
                } else if (existing.getType() != RedisValue.Type.HASH) {
                    return existing;
                } else {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, String> existingHash = (java.util.Map<String, String>) existing.getData();
                    hash = new java.util.HashMap<>(existingHash);
                }
                for (int i = 2; i + 1 < command.size(); i += 2) {
                    hash.put(command.get(i), command.get(i + 1));
                }
                return RedisValue.hash(hash);
            });
        }

        private void handleSAddCommand(List<String> command, RedisDatabase db) {
            // SADD key member [member ...]
            if (command.size() < 3) return;
            String key = command.get(1);

            db.compute(key, existing -> {
                java.util.Set<String> set;
                if (existing == null) {
                    set = new java.util.HashSet<>();
                } else if (existing.getType() != RedisValue.Type.SET) {
                    return existing;
                } else {
                    @SuppressWarnings("unchecked")
                    java.util.Set<String> existingSet = (java.util.Set<String>) existing.getData();
                    set = new java.util.HashSet<>(existingSet);
                }
                for (int i = 2; i < command.size(); i++) {
                    set.add(command.get(i));
                }
                return RedisValue.set(set);
            });
        }

        private void handleZAddCommand(List<String> command, RedisDatabase db) {
            // ZADD key score member [score member ...]
            if (command.size() < 4) return;
            String key = command.get(1);

            db.compute(key, existing -> {
                java.util.Map<String, Double> zset;
                if (existing == null) {
                    zset = new java.util.LinkedHashMap<>();
                } else if (existing.getType() != RedisValue.Type.SORTED_SET) {
                    return existing;
                } else {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Double> existingZset = (java.util.Map<String, Double>) existing.getData();
                    zset = new java.util.LinkedHashMap<>(existingZset);
                }
                for (int i = 2; i + 1 < command.size(); i += 2) {
                    try {
                        double score = Double.parseDouble(command.get(i));
                        String member = command.get(i + 1);
                        zset.put(member, score);
                    } catch (NumberFormatException ignored) {}
                }
                return RedisValue.sortedSet(zset);
            });
        }

        // ==================== Error Handling ====================

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[Replication] Master connection error: " + cause.getMessage());
            cause.printStackTrace();
            cleanupRdbState();
            ctx.close();
            connected = false;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("[Replication] Master connection closed");
            cleanupRdbState();
            connected = false;
        }

        /**
         * Cleans up RDB loading state and releases any allocated buffers.
         * Must be called on connection teardown to prevent ByteBuf leaks.
         */
        private void cleanupRdbState() {
            if (loadingRdb) {
                loadingRdb = false;
                expectedRdbSize = -1;
                if (rdbBuffer != null) {
                    rdbBuffer.release();
                    rdbBuffer = null;
                }
            }
        }
    }

    // ==================== Lifecycle Management ====================

    /**
     * Disconnects from the master and releases resources.
     */
    public void disconnect() {
        connected = false;
        if (channel != null && channel.isActive()) {
            channel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    // ==================== Status Methods ====================

    public boolean isConnected() {
        return connected && channel != null && channel.isActive();
    }

    public HandshakeState getHandshakeState() {
        return handshakeState;
    }

    public long getBytesProcessed() {
        return bytesProcessed.get();
    }

    public void addBytesProcessed(long bytes) {
        bytesProcessed.addAndGet(bytes);
    }
}
