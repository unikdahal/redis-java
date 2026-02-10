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
     * Create a tracker for a replica connection and initialize its replication state.
     *
     * <p>Initializes the connection state to CONNECTING, acknowledged and expected offsets to 0,
     * capabilities to an empty list, and listening port to -1.</p>
     *
     * @param channel the Netty channel to the replica
     * @param host the replica's hostname or IP address
     * @param port the replica's port number
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
     * Sends a RESP-encoded command to the replica and increments the connection's expected offset.
     *
     * <p>If the underlying channel is not active or the replica is not in the STREAMING state, the
     * method does nothing and returns {@code false}. When a write is attempted, the expected offset
     * is incremented by the command byte length before sending.
     *
     * @param respCommand the RESP-encoded command bytes to send
     * @return {@code true} if a write was attempted, {@code false} if the channel was not ready or the replica was not streaming
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
     * Propagates a RESP-encoded command to the replica.
     *
     * @param respCommand RESP-encoded command as a UTF-8 string
     * @return `true` if a write was attempted to the replica, `false` otherwise
     */
    public boolean propagateCommand(String respCommand) {
        return propagateCommand(respCommand.getBytes(StandardCharsets.UTF_8));
    }

    // ==================== Offset Management ====================

    /**
     * Record the replica's acknowledged replication offset reported via REPLCONF ACK.
     *
     * @param offset the replica's acknowledged byte offset; callers should provide a value greater than or equal to the previous acknowledgement
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
     * Returns the number of bytes sent to the replica that have not yet been acknowledged.
     *
     * @return the number of unacknowledged bytes (zero or greater)
     */
    public long getReplicationLag() {
        return Math.max(0, expectedOffset.get() - acknowledgedOffset.get());
    }

    // ==================== State Management ====================

    /**
     * Set the replica's lifecycle state.
     *
     * @param state the new lifecycle state for this replica
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
     * Record a capability announced by the replica in the negotiated capabilities list.
     *
     * This method is safe to call concurrently.
     *
     * @param capability the capability name (e.g., "psync2")
     */
    public void addCapability(String capability) {
        capabilities.add(capability);
    }

    /**
     * Determines if the replica announced the given capability.
     *
     * @param capability capability identifier to check
     * @return `true` if the replica announced this capability, `false` otherwise
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
     * Get the replica's listening port.
     *
     * @return the listening port, or -1 if the replica has not reported a listening port
     */
    public int getListeningPort() {
        return listeningPort;
    }

    /**
     * Netty channel used to communicate with the replica.
     *
     * @return the Netty Channel for this replica connection
     */

    public Channel getChannel() {
        return channel;
    }

    /**
     * Host name or IP address of the replica.
     *
     * @return the replica's host name or IP address
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the configured remote port for this replica connection.
     *
     * @return the configured remote port (the port provided at construction); note this is not the replica-reported listening port returned by {@code getListeningPort()}.
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the latest byte offset the replica has acknowledged processing.
     *
     * @return the latest acknowledged byte offset from the replica (0 if none reported)
     */
    public long getAcknowledgedOffset() {
        return acknowledgedOffset.get();
    }

    /**
     * Get the total number of bytes sent to the replica that the master expects to be acknowledged.
     *
     * @return the total number of bytes sent to the replica and awaiting acknowledgment
     */
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

    /**
     * Human-readable representation of the replica connection including host, effective port,
     * state, and current replication lag.
     *
     * @return a string containing the replica's host, effective port (listening port if reported,
     *         otherwise configured port), current state, and replication lag in bytes
     */

    @Override
    public String toString() {
        return String.format("ReplicaConnection{host=%s, port=%d, state=%s, lag=%d}",
            host, listeningPort > 0 ? listeningPort : port, state, getReplicationLag());
    }
}