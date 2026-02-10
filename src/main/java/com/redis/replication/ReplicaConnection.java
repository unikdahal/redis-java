package com.redis.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a connected replica (slave) from the master's perspective.
 * <p>
 * This class tracks the state and offset of a single replica connection,
 * providing efficient command propagation and acknowledgment tracking.
 *
 * <h2>State Machine</h2>
 * <pre>
 *     CONNECTING
 *          │
 *          ▼
 *      HANDSHAKE ←────────────────┐
 *          │                      │
 *          ▼                      │
 *   SYNC_REQUESTED                │
 *          │                      │
 *          ▼                      │
 *    LOADING_RDB                  │
 *          │                      │
 *          ▼                      │
 *     STREAMING ──(disconnect)───►│
 *          │                      │
 *          ▼                      │
 *    DISCONNECTED ────────────────┘
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * <ul>
 *   <li><b>Offset Tracking:</b> AtomicLong for lock-free operations</li>
 *   <li><b>State:</b> Volatile for visibility</li>
 *   <li><b>Capabilities:</b> CopyOnWriteArrayList for thread-safe iteration</li>
 *   <li><b>Channel:</b> Netty channels are thread-safe for writes</li>
 * </ul>
 *
 * <h2>Offset Tracking</h2>
 * Two offsets are tracked:
 * <ul>
 *   <li><b>expectedOffset:</b> Bytes we've sent to this replica</li>
 *   <li><b>acknowledgedOffset:</b> Bytes the replica has confirmed processing</li>
 *   <li><b>Lag:</b> expectedOffset - acknowledgedOffset (bytes pending ack)</li>
 * </ul>
 *
 * @see ReplicationManager
 * @see ReplicaState
 */
public class ReplicaConnection {

    // ==================== Connection Identity ====================

    /**
     * The Netty channel for this replica connection.
     * Used for writing propagated commands.
     */
    private final Channel channel;

    /**
     * Remote host address of the replica.
     * Extracted from channel for logging/debugging.
     */
    private final String host;

    /**
     * Remote port of the replica (from initial connection).
     */
    private final int port;

    // ==================== Replication State ====================

    /**
     * Current state in the replication state machine.
     * Volatile ensures visibility across threads.
     */
    private volatile ReplicaState state;

    /**
     * Offset that this replica has acknowledged processing.
     * Updated when we receive REPLCONF ACK from the replica.
     */
    private final AtomicLong acknowledgedOffset;

    /**
     * Offset we expect this replica to reach (bytes we've sent).
     * Updated before each command propagation.
     */
    private final AtomicLong expectedOffset;

    // ==================== Capabilities ====================

    /**
     * Capabilities negotiated during REPLCONF handshake.
     * Examples: "psync2", "eof"
     *
     * Uses CopyOnWriteArrayList for thread-safe reads during handshake.
     */
    private final List<String> capabilities;

    /**
     * The listening port reported by the replica via REPLCONF.
     * May differ from connection port if replica is behind NAT.
     */
    private volatile int listeningPort;

    // ==================== State Enum ====================

    /**
     * States in the replica connection lifecycle.
     */
    public enum ReplicaState {
        /** Initial TCP connection established */
        CONNECTING,

        /** REPLCONF exchange in progress */
        HANDSHAKE,

        /** PSYNC sent, awaiting response */
        SYNC_REQUESTED,

        /** Receiving RDB file */
        LOADING_RDB,

        /** Normal replication streaming */
        STREAMING,

        /** Connection lost or closed */
        DISCONNECTED
    }

    // ==================== Constructor ====================

    /**
     * Creates a new replica connection tracker.
     *
     * @param channel The Netty channel to the replica
     * @param host The replica's hostname/IP
     * @param port The replica's port
     */
    public ReplicaConnection(Channel channel, String host, int port) {
        this.channel = channel;
        this.host = host;
        this.port = port;
        this.state = ReplicaState.CONNECTING;
        this.acknowledgedOffset = new AtomicLong(0);
        this.expectedOffset = new AtomicLong(0);
        this.capabilities = new CopyOnWriteArrayList<>();
        this.listeningPort = -1;
    }

    // ==================== Command Propagation ====================

    /**
     * Propagates a command to this replica using zero-copy ByteBuf.
     *
     * <p><b>Performance Notes:</b>
     * <ul>
     *   <li>Uses Unpooled.wrappedBuffer for zero-copy</li>
     *   <li>Updates expectedOffset before write (conservative)</li>
     *   <li>Non-blocking write via Netty's event loop</li>
     * </ul>
     *
     * @param respCommand The RESP-encoded command bytes
     * @return true if write was attempted, false if channel not ready
     */
    public boolean propagateCommand(byte[] respCommand) {
        // Guard: only propagate to active, streaming replicas
        if (!channel.isActive() || state != ReplicaState.STREAMING) {
            return false;
        }

        // Update expected offset BEFORE sending (conservative tracking)
        expectedOffset.addAndGet(respCommand.length);

        // Zero-copy wrap and write
        ByteBuf buf = Unpooled.wrappedBuffer(respCommand);
        channel.writeAndFlush(buf);

        return true;
    }

    /**
     * Propagates a command (string version).
     * Convenience method that converts to UTF-8 bytes.
     *
     * @param respCommand The RESP-encoded command string
     * @return true if write was attempted
     */
    public boolean propagateCommand(String respCommand) {
        return propagateCommand(respCommand.getBytes(StandardCharsets.UTF_8));
    }

    // ==================== Offset Management ====================

    /**
     * Updates the acknowledged offset from REPLCONF ACK.
     * Called when replica reports bytes it has processed.
     *
     * @param offset The offset value from REPLCONF ACK
     */
    public void updateAcknowledgedOffset(long offset) {
        // Use set() for simplicity; ACKs should be monotonically increasing
        acknowledgedOffset.set(offset);
    }

    /**
     * Checks if this replica has acknowledged up to the given offset.
     * Used by WAIT command to check synchronization.
     *
     * @param offset The target offset to check against
     * @return true if replica has acknowledged >= offset
     */
    public boolean hasAcknowledged(long offset) {
        return acknowledgedOffset.get() >= offset;
    }

    /**
     * Calculates the replication lag (bytes not yet acknowledged).
     *
     * @return Bytes sent but not yet acknowledged (always >= 0)
     */
    public long getReplicationLag() {
        return Math.max(0, expectedOffset.get() - acknowledgedOffset.get());
    }

    // ==================== State Management ====================

    /**
     * Updates the replica's state in the state machine.
     *
     * @param state The new state
     */
    public void setState(ReplicaState state) {
        this.state = state;
    }

    /**
     * Gets the current replica state.
     *
     * @return Current ReplicaState
     */
    public ReplicaState getState() {
        return state;
    }

    // ==================== Capability Management ====================

    /**
     * Adds a capability announced by the replica.
     *
     * @param capability The capability name (e.g., "psync2")
     */
    public void addCapability(String capability) {
        capabilities.add(capability);
    }

    /**
     * Checks if replica has a specific capability.
     *
     * @param capability The capability to check
     * @return true if replica announced this capability
     */
    public boolean hasCapability(String capability) {
        return capabilities.contains(capability);
    }

    // ==================== Port Management ====================

    /**
     * Sets the listening port reported by REPLCONF.
     *
     * @param port The replica's listening port
     */
    public void setListeningPort(int port) {
        this.listeningPort = port;
    }

    /**
     * Gets the replica's listening port.
     * Returns -1 if not yet reported.
     *
     * @return Listening port or -1
     */
    public int getListeningPort() {
        return listeningPort;
    }

    // ==================== Getters ====================

    public Channel getChannel() {
        return channel;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public long getAcknowledgedOffset() {
        return acknowledgedOffset.get();
    }

    public long getExpectedOffset() {
        return expectedOffset.get();
    }

    // ==================== Lifecycle ====================

    /**
     * Closes the replica connection and marks it as disconnected.
     */
    public void close() {
        state = ReplicaState.DISCONNECTED;
        if (channel.isActive()) {
            channel.close();
        }
    }

    // ==================== Object Methods ====================

    @Override
    public String toString() {
        return String.format("ReplicaConnection{host=%s, port=%d, state=%s, lag=%d}",
            host, listeningPort > 0 ? listeningPort : port, state, getReplicationLag());
    }
}
