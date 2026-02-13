package com.redis.replication;

/**
 * Represents the role of a Redis server in the replication topology.
 * <p>
 * This is a fundamental concept in Redis replication architecture:
 *
 * <h2>Role Responsibilities</h2>
 * <table border="1">
 *   <tr><th>Role</th><th>Accepts Writes</th><th>Owns Replication Log</th><th>Behavior</th></tr>
 *   <tr><td>MASTER</td><td>Yes</td><td>Yes</td><td>Executes commands, propagates to replicas</td></tr>
 *   <tr><td>SLAVE</td><td>No (READONLY)</td><td>No</td><td>Replays commands, tracks offset</td></tr>
 * </table>
 *
 * <h2>State Machine (Role Transitions)</h2>
 * <pre>
 * ┌──────────────────────────────────────────────────────────────┐
 * │                                                              │
 * │    ┌────────┐    SLAVEOF host port    ┌────────┐            │
 * │    │ MASTER │ ──────────────────────► │ SLAVE  │            │
 * │    └────────┘                         └────────┘            │
 * │         ▲                                  │                 │
 * │         │         SLAVEOF NO ONE           │                 │
 * │         └──────────────────────────────────┘                 │
 * │              (Promotion to master)                           │
 * └──────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Write Protection</h2>
 * When role is SLAVE, write commands are rejected with {@code -READONLY} error.
 * This is enforced at the protocol handler level (RedisCommandHandler).
 *
 * @see ReplicationManager#isMaster()
 * @see ReplicationManager#isSlave()
 */
public enum ServerRole {

    /**
     * Master role: accepts writes, owns replication log, propagates to replicas.
     * <p>
     * Masters are the authoritative source of data. All write operations
     * must go through the master, which then propagates canonicalized
     * commands to all connected replicas.
     */
    MASTER,

    /**
     * Slave (replica) role: read-only, replays commands from master.
     * <p>
     * Replicas reject write commands with {@code -READONLY} error.
     * They receive commands from the master's replication stream and
     * replay them to maintain a consistent copy of the dataset.
     * <p>
     * Note: Redis uses "slave" terminology internally for protocol compatibility,
     * but "replica" is the preferred term in documentation.
     */
    SLAVE
}
