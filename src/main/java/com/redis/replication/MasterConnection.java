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
     * Establishes a TCP connection to the configured master and begins the replication handshake.
     *
     * <p>On successful connection the method starts the handshake sequence (PING → REPLCONF → PSYNC).
     *
     * @return a CompletableFuture that completes when the connection to the master is established,
     *         or completes exceptionally if the connection attempt fails
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
             /**
              * Initializes a newly accepted SocketChannel's pipeline for communication with the master.
              *
              * Adds a MasterResponseHandler to the channel pipeline to handle inbound replication messages
              * and protocol parsing.
              *
              * @param ch the SocketChannel whose pipeline will be configured
              */
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

        /**
         * Decodes inbound bytes from the master, handling RDB streaming and RESP responses.
         *
         * During each invocation this method:
         * - If currently receiving an RDB, feeds bytes to the RDB handler until the RDB is complete; if more data is required it resets the reader index and returns.
         * - Otherwise parses a single RESP response; if the buffer does not contain a full RESP value it resets the reader index and returns.
         * - When a complete RESP response is parsed, routes the parsed arguments to the handshake/streaming response handler.
         *
         * The method consumes bytes from `in` as responses or RDB data are completed and may update the connection's handshake and streaming state via the response handlers.
         *
         * @param ctx the Netty channel handler context
         * @param in  the inbound byte buffer to read from; reader index may be advanced or reset when incomplete data is encountered
         * @param out the list to which decoded messages would be added (not used directly by this decoder)
         */
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
         * Parse a single RESP value from the provided buffer and append its textual parts to result.
         *
         * If the buffer does not contain a complete RESP value, the reader index is not advanced and the method returns `false`.
         *
         * @param buf the ByteBuf containing RESP-encoded data; may be advanced when a full value is parsed
         * @param result list to receive parsed string parts (bulk strings, simple strings, integers, and array elements) in encounter order
         * @return `true` if a complete RESP value was parsed and appended to result, `false` if more data is required
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
         * Parses a RESP simple string, error, or integer terminated by CRLF and appends the parsed text to the result list.
         *
         * @param buf    the ByteBuf positioned at the start of the RESP simple value
         * @param result the list to which the parsed string will be appended
         * @return `true` if a complete CRLF-terminated simple value was parsed and added to {@code result}, `false` otherwise
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
         * Parses a RESP bulk string from the buffer and appends its value to the result list.
         *
         * If the bulk length is `-1` (null bulk string), `null` is appended to `result`.
         *
         * @param buf the ByteBuf to read the bulk string from; reader index is advanced on success and reset on incomplete input
         * @param result list to which the parsed string (or `null` for a null bulk) will be appended
         * @return `true` if a complete bulk string was parsed and appended to `result`, `false` if more bytes are required
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
         * Parses a RESP array from the buffer and appends its elements (flattened) to the provided result list.
         *
         * The method expects an array header of the form `*<count>\r\n` followed by <count> RESP elements.
         * If the array count is `-1`, a single `null` is added to `result`.
         * On successful parse the buffer's reader index is advanced past the entire array; if data is incomplete the
         * reader index is restored to its original position and the method returns `false`.
         *
         * @param buf the ByteBuf containing RESP data; its reader index is advanced on successful parse
         * @param result the list to receive parsed string elements (array elements are flattened into this list)
         * @return `true` if a complete array was parsed and appended to `result`, `false` if more data is required or parsing failed
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
         * Parses a CRLF-terminated inline (space-separated) command from the buffer and appends its parts to `result`.
         *
         * @param buf    the ByteBuf to read from; parsing requires a CRLF-terminated line
         * @param result destination list to which command parts (split on spaces) will be added
         * @return       `true` if a complete CRLF-terminated inline command was read and added to `result`, `false` if more bytes are needed
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

        /**
         * Handle the master's response to the initial PING during the replication handshake.
         *
         * If the response equals "PONG" (case-insensitive), advance the handshake state to
         * REPLCONF_PORT_SENT and send a REPLCONF listening-port command with the local listening port.
         *
         * @param response the master's reply to PING (expected value: "PONG")
         */
        private void handlePingResponse(String response) {
            if ("PONG".equalsIgnoreCase(response)) {
                System.out.println("[Replication] Master responded to PING");
                handshakeState = HandshakeState.REPLCONF_PORT_SENT;
                sendCommand("REPLCONF", "listening-port", String.valueOf(listeningPort));
            }
        }

        /**
         * Processes the master's reply to the REPLCONF listening-port command.
         *
         * If the master responds with "OK" (case-insensitive), advances the handshake to
         * REPLCONF_CAPA_SENT and sends a REPLCONF capabilities message requesting PSYNC2.
         *
         * @param response the master's textual response to the REPLCONF listening-port request
         */
        private void handleReplconfPortResponse(String response) {
            if ("OK".equalsIgnoreCase(response)) {
                System.out.println("[Replication] REPLCONF listening-port acknowledged");
                handshakeState = HandshakeState.REPLCONF_CAPA_SENT;
                sendCommand("REPLCONF", "capa", "psync2");
            }
        }

        /**
         * Handles the server's response to the REPLCONF CAPA command.
         *
         * If the response equals "OK" (case-insensitive), advances the handshake to
         * request PSYNC from the master.
         *
         * @param response the server reply to the REPLCONF CAPA command
         */
        private void handleReplconfCapaResponse(String response) {
            if ("OK".equalsIgnoreCase(response)) {
                System.out.println("[Replication] REPLCONF capa acknowledged");
                handshakeState = HandshakeState.PSYNC_SENT;
                sendCommand("PSYNC", "?", "-1");
            }
        }

        /**
         * Process the master's PSYNC reply and advance the handshake and RDB reception state.
         *
         * <p>If the response starts with {@code FULLRESYNC <replid> <offset>}, the method extracts the
         * replication ID and offset, records the master's replication offset, and transitions to RDB
         * loading mode (sets {@code loadingRdb=true} and {@code expectedRdbSize=-1}). If the response
         * starts with {@code CONTINUE}, the method transitions to streaming mode and disables RDB loading.
         *
         * @param response the raw PSYNC reply line received from the master (e.g. {@code "FULLRESYNC <replid> <offset>"} or {@code "CONTINUE"})
         */
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
         * Accumulates RDB bytes from the provided input buffer until the full RDB file has been received.
         *
         * When the total size is not yet known this method reads the Redis bulk-string size marker and
         * initializes an internal buffer for the expected RDB size. It appends available bytes into the
         * buffer and, once the expected size is reached, processes the RDB, releases the buffer, clears
         * the RDB-loading flag, and transitions the handshake state to STREAMING.
         *
         * @param in the incoming ByteBuf containing RDB data (may contain partial or multiple chunks)
         * @return `true` if the full RDB file has been received and processed, `false` if additional data is required
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
         * Handle a complete RDB file received from the master.
         *
         * <p>Currently this implementation only acknowledges receipt and does not restore database state.
         *
         * @param rdb the ByteBuf containing the complete RDB file bytes
         */
        private void processRdbFile(ByteBuf rdb) {
            System.out.println("[Replication] RDB processing complete (empty database mode)");
        }

        // ==================== Streaming Command Handling ====================

        /**
         * Routes and handles a single replicated command received from the master.
         *
         * If the command is empty the method returns immediately. If the command is
         * "REPLCONF GETACK" the method replies to the master with "REPLCONF ACK <offset>"
         * using the current replication byte offset; otherwise the command is applied
         * locally via executeReplicatedCommand.
         *
         * @param command a list of RESP-parsed tokens where the first element is the command name
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
         * Apply a replicated write command to the local RedisDatabase.
         *
         * <p>Handles a minimal set of replication write commands received from the master.
         * Supported commands:
         * <ul>
         *   <li>SET key value [PX milliseconds | EX seconds] — stores a value with optional expiry</li>
         *   <li>DEL key [key ...] — deletes one or more keys</li>
         *   <li>INCR key — increments the numeric value of a key (creates key with 0 before increment)</li>
         * </ul>
         * Unsupported or malformed commands are ignored.
         *
         * @param command a List of strings where the first element is the command name and the remaining elements are its arguments (e.g., [\"SET\", \"key\", \"value\"])
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

        /**
         * Handles exceptions raised by the Netty pipeline for the master connection.
         *
         * Logs the error, closes the channel context, and marks the connection as not connected.
         *
         * @param ctx   the Netty channel handler context where the exception occurred
         * @param cause the exception that was thrown
         */

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[Replication] Master connection error: " + cause.getMessage());
            cause.printStackTrace();
            ctx.close();
            connected = false;
        }

        /**
         * Handle the channel becoming inactive by marking the master connection as disconnected and logging the closure.
         *
         * @param ctx the Netty ChannelHandlerContext for the closed channel
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("[Replication] Master connection closed");
            connected = false;
        }
    }

    // ==================== Lifecycle Management ====================

    /**
     * Close the replication connection and release associated network resources.
     *
     * Marks this connection as disconnected, closes the active Netty channel if present,
     * and shuts down the worker event loop group.
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

    /**
     * Indicates whether the connection to the master is currently active.
     *
     * @return `true` if the client has an active channel to the master, `false` otherwise.
     */

    public boolean isConnected() {
        return connected && channel != null && channel.isActive();
    }

    /**
     * Current handshake state of the connection to the master.
     *
     * @return the current HandshakeState representing replication handshake progress
     */
    public HandshakeState getHandshakeState() {
        return handshakeState;
    }

    /**
     * Get the total number of bytes processed from the replication stream.
     *
     * @return the total number of bytes processed from the replication stream
     */
    public long getBytesProcessed() {
        return bytesProcessed.get();
    }

    /**
     * Atomically increments the running total of replication bytes processed.
     *
     * @param bytes the number of bytes to add to the processed counter (in bytes); can be negative to decrement the counter
     */
    public void addBytesProcessed(long bytes) {
        bytesProcessed.addAndGet(bytes);
    }
}