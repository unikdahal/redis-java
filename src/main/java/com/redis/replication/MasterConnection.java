package com.redis.replication;

import com.redis.storage.RedisDatabase;
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
     *
     * @param args Command name followed by arguments
     */
    private void sendCommand(String... args) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }

        ByteBuf buf = Unpooled.copiedBuffer(sb.toString(), StandardCharsets.UTF_8);
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
                in.markReaderIndex();

                // Special handling for RDB data
                if (loadingRdb) {
                    if (!handleRdbData(in)) {
                        in.resetReaderIndex();
                        return;
                    }
                    continue;
                }

                // Parse RESP response
                argsBuffer.clear();
                if (!parseRespResponse(in, argsBuffer)) {
                    in.resetReaderIndex();
                    return;
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
            int start = buf.readerIndex();
            int end = buf.indexOf(start, buf.writerIndex(), (byte) '\r');
            if (end == -1 || buf.readableBytes() < (end - start + 2)) return false;

            CharSequence lenStr = buf.readCharSequence(end - start, StandardCharsets.UTF_8);
            buf.skipBytes(2); // Skip \r\n

            int len = Integer.parseInt(lenStr.toString());
            if (len == -1) {
                result.add(null);
                return true;
            }

            if (buf.readableBytes() < len + 2) {
                buf.readerIndex(start);
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
            int start = buf.readerIndex();
            int end = buf.indexOf(start, buf.writerIndex(), (byte) '\r');
            if (end == -1 || buf.readableBytes() < (end - start + 2)) return false;

            CharSequence lenStr = buf.readCharSequence(end - start, StandardCharsets.UTF_8);
            buf.skipBytes(2); // Skip \r\n

            int numElements = Integer.parseInt(lenStr.toString());
            if (numElements == -1) {
                result.add(null);
                return true;
            }

            for (int i = 0; i < numElements; i++) {
                List<String> element = new ArrayList<>();
                if (!parseRespResponse(buf, element)) {
                    buf.readerIndex(start);
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
                    long offset = Long.parseLong(parts[2]);
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
         * @param in Input buffer
         * @return true if RDB is complete, false if more data needed
         */
        private boolean handleRdbData(ByteBuf in) {
            // First, read the RDB size if not yet known
            if (expectedRdbSize == -1) {
                if (in.readableBytes() < 1) return false;

                byte marker = in.readByte();
                if (marker != '$') {
                    System.err.println("[Replication] Expected RDB size marker, got: " + (char) marker);
                    return false;
                }

                int start = in.readerIndex();
                int end = in.indexOf(start, in.writerIndex(), (byte) '\r');
                if (end == -1) {
                    in.readerIndex(in.readerIndex() - 1);
                    return false;
                }

                CharSequence sizeStr = in.readCharSequence(end - start, StandardCharsets.UTF_8);
                in.skipBytes(2); // Skip \r\n

                expectedRdbSize = Integer.parseInt(sizeStr.toString());
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
         * This is a simplified implementation. A production version
         * would use the command registry for consistency.
         */
        private void executeReplicatedCommand(List<String> command) {
            if (command.isEmpty()) return;

            String cmdName = command.get(0).toUpperCase();
            RedisDatabase db = RedisDatabase.getInstance();

            switch (cmdName) {
                case "SET" -> {
                    if (command.size() >= 3) {
                        String key = command.get(1);
                        String value = command.get(2);
                        // Handle expiry options
                        if (command.size() >= 5 && "PX".equalsIgnoreCase(command.get(3))) {
                            long px = Long.parseLong(command.get(4));
                            db.put(key, value, px);
                        } else if (command.size() >= 5 && "EX".equalsIgnoreCase(command.get(3))) {
                            long ex = Long.parseLong(command.get(4)) * 1000;
                            db.put(key, value, ex);
                        } else {
                            db.put(key, value);
                        }
                    }
                }
                case "DEL" -> {
                    for (int i = 1; i < command.size(); i++) {
                        db.remove(command.get(i));
                    }
                }
                case "INCR" -> {
                    if (command.size() >= 2) {
                        String key = command.get(1);
                        String current = db.get(key);
                        long val = current == null ? 0 : Long.parseLong(current);
                        db.put(key, String.valueOf(val + 1));
                    }
                }
                // Additional commands can be added here
            }
        }

        // ==================== Error Handling ====================

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[Replication] Master connection error: " + cause.getMessage());
            cause.printStackTrace();
            ctx.close();
            connected = false;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("[Replication] Master connection closed");
            connected = false;
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
