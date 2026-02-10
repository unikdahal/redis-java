# Copilot Instructions for Redis Java Server

## High-Level Overview

This repository contains a lightweight, high-performance, in-memory Redis-compatible server built with **Java 25** and **Netty**. The project implements a comprehensive subset of Redis commands using the Redis Serialization Protocol (RESP) for communication. It features full master-replica replication with deterministic command propagation.

**Key Technologies:**
- Java 25 with preview features enabled
- Netty 4.1.118.Final (asynchronous, event-driven networking)
- Maven 3.9+ for build management
- JUnit 5 and Mockito for testing
- RESP (Redis Serialization Protocol) for client-server communication

**Supported Command Categories:**
- String operations: SET, GET, INCR
- List operations: LPUSH, RPUSH, LPOP, LLEN, LRANGE, BLPOP
- Stream operations: XADD, XRANGE, XREAD
- Transactions: MULTI, EXEC, DISCARD
- Key management: DEL, EXPIRE, TTL, TYPE
- Replication: REPLCONF, PSYNC, INFO

## Build Instructions

### Prerequisites
- Java 25 or higher (preferred) or Java 17+ (current environment)
- Maven 3.9 or higher

### Build Commands (in order)

1. **Clean and build the project:**
   ```bash
   mvn clean install
   ```
   - This compiles the code, runs all tests, and creates `target/redis-server.jar`
   - Expected time: 30-60 seconds
   - **Important:** The project uses `--enable-preview` flag for Java 25 preview features

2. **Run tests only:**
   ```bash
   mvn test
   ```
   - Runs all JUnit 5 tests
   - Expected time: 10-30 seconds

3. **Run tests with verbose output:**
   ```bash
   mvn test -X
   ```

4. **Build without tests:**
   ```bash
   mvn clean package -DskipTests
   ```

### Running the Server

```bash
java --enable-preview -jar target/redis-server.jar
```
- Default port: 6379
- The server must be built first using `mvn clean install`

### Testing with Redis CLI

Connect using any Redis client:
```bash
redis-cli -p 6379
```

Or use the provided test script:
```bash
bash src/main/test/test.sh
```
(Requires the server to be running and `netcat` (nc) to be installed)
(Note: The test script is located in `src/main/test/` which is unconventional but part of the project structure)

## Project Structure

```
redis-java/
├── pom.xml                          # Maven build configuration
├── README.md                        # Project documentation
├── .github/
│   └── copilot-instructions.md     # This file
├── src/
│   ├── main/java/com/redis/
│   │   ├── commands/                # Command implementations
│   │   │   ├── ICommand.java        # Command interface
│   │   │   ├── CommandRegistry.java # Command lookup registry
│   │   │   ├── string/              # String commands (SET, GET, INCR)
│   │   │   ├── list/                # List commands (LPUSH, RPUSH, LPOP, etc.)
│   │   │   ├── stream/              # Stream commands (XADD, XRANGE, XREAD)
│   │   │   ├── generic/             # Generic commands (DEL, EXPIRE, TYPE, etc.)
│   │   │   ├── transaction/         # Transaction commands (MULTI, EXEC, DISCARD)
│   │   │   └── replication/         # Replication commands (REPLCONF, PSYNC, INFO)
│   │   ├── config/
│   │   │   └── RedisConfig.java     # Server configuration
│   │   ├── server/
│   │   │   ├── NettyRedisServer.java    # Main server class (entry point)
│   │   │   └── RedisCommandHandler.java # RESP protocol handler + replica write protection
│   │   ├── storage/
│   │   │   ├── RedisDatabase.java   # In-memory data store (ConcurrentHashMap)
│   │   │   ├── RedisValue.java      # Type-safe value wrapper (sealed interface)
│   │   │   └── ExpiryManager.java   # Key expiration manager (DelayQueue)
│   │   ├── replication/
│   │   │   ├── ReplicationManager.java  # Central replication state and operations
│   │   │   ├── ReplicationLog.java      # Lock-free ring buffer for partial resync
│   │   │   ├── SnapshotProducer.java    # RDB snapshot generation for full resync
│   │   │   ├── CommandPropagator.java   # Command canonicalization and propagation
│   │   │   ├── ReplicaConnection.java   # Per-replica connection state
│   │   │   ├── MasterConnection.java    # Replica-to-master connection handler
│   │   │   ├── RdbGenerator.java        # RDB file format generation
│   │   │   └── ServerRole.java          # MASTER/SLAVE enum
│   │   ├── transaction/
│   │   │   └── TransactionContext.java  # Per-connection transaction state
│   │   └── util/
│   │       ├── ExpiryTask.java      # Expiry task implementation
│   │       └── StreamId.java        # Stream entry ID handling
│   ├── main/test/test.sh            # Manual integration test script
│   └── test/java/com/redis/
│       ├── commands/                # Unit tests for commands
│       ├── replication/             # Replication unit tests
│       │   ├── ReplicationLogTest.java
│       │   └── SnapshotProducerTest.java
│       ├── integration/             # Integration tests
│       └── storage/                 # Unit tests for storage
└── target/
    └── redis-server.jar             # Executable JAR (generated after build)
```

## Architecture

The server follows a layered architecture:

1. **Network Layer (Netty):** `NettyRedisServer` accepts connections using Netty's boss and worker thread pools
2. **Protocol Layer:** `RedisCommandHandler` parses RESP protocol and delegates to command registry
3. **Command Layer:** Individual command implementations in `commands/` subdirectories (string/, list/, stream/, generic/, transaction/, replication/)
4. **Storage Layer:** `RedisDatabase` manages in-memory key-value storage with `ConcurrentHashMap`
5. **Expiration Layer:** `ExpiryManager` handles TTL-based key expiration using `DelayQueue`
6. **Replication Layer:** `ReplicationManager` + `CommandPropagator` + `ReplicationLog` + `SnapshotProducer` handle master-replica replication

### Replication Architecture (Production-Grade)

The replication system implements a **single-writer, log-based state machine** with snapshot checkpoints:

```
parse → validate → canonicalize → execute → append to log → send to replicas
```

**Core Roles:**
| Role | Responsibilities |
|:-----|:-----------------|
| **Master** | Accepts writes, executes commands, owns replication log, propagates to replicas |
| **Replica** | Rejects writes (READONLY), replays commands, tracks offset, requests PSYNC |
| **SnapshotProducer** | Creates point-in-time state snapshots independent of live mutations |

**Key Components:**
- `ReplicationManager` - Manages master/replica roles, replica connections, offset tracking, circuit breakers
- `ReplicationLog` - Lock-free ring buffer (64KB-512MB) for partial resync support
- `SnapshotProducer` - Generates RDB snapshots for full resync
- `CommandPropagator` - Determines which commands to propagate and handles RESP encoding
- `ReplicaConnection` - Per-replica state tracking with backpressure handling
- `ICommand.getReplicationArgs()` - Commands can override to provide canonical arguments
- `ICommand.getReplicationCommandName()` - Commands can override to use different command for replication

**Optimizations Over Redis:**
| Feature | Redis | This Implementation |
|:--------|:------|:--------------------|
| Offset Tracking | Mutex-protected long | Lock-free `AtomicLong` |
| Statistics | Atomic increments | `LongAdder` (10x faster) |
| Backlog | Single producer lock | Lock-free ring buffer |
| Propagation | Synchronous per-replica | Async with circuit breaker |
| WAIT Polling | Fixed interval | Adaptive exponential backoff |
| Failure Handling | Simple disconnect | Circuit breaker + recovery |

**Command Canonicalization:**
Non-deterministic commands are rewritten before replication:
- `XADD stream * field value` → `XADD stream <actual-id> field value`
- `SET key value EX 60` → `SET key value PXAT <timestamp>`
- `EXPIRE key 60` → `PEXPIREAT key <timestamp>`

**Full Sync vs Partial Sync Decision:**
```
ID mismatch → FULL SYNC
offset not in backlog → FULL SYNC
else → PARTIAL SYNC
```
No heuristics. No guessing. The decision is deterministic.

**Write Protection on Replicas:**
Replicas automatically reject write commands with `-READONLY` error. This is enforced in `RedisCommandHandler`.

## Key Development Guidelines

### When Making Changes

1. **Always enable preview features:** The project uses Java preview features. Ensure compiler and runtime flags include `--enable-preview`

2. **Thread Safety:** The codebase uses `ConcurrentHashMap` for thread-safe operations. Maintain this pattern when adding new storage features

3. **RESP Protocol:** All client-server communication uses RESP format:
   - Simple Strings: `+OK\r\n`
   - Errors: `-ERR message\r\n`
   - Integers: `:1\r\n`
   - Bulk Strings: `$6\r\nfoobar\r\n`
   - Null: `$-1\r\n`
   - Arrays: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

4. **Command Implementation Pattern:**
   - Implement `ICommand` interface
   - Register in `CommandRegistry`
   - Add unit tests in `src/test/java/com/redis/commands/`
   - For write commands:
     - Override `isWriteCommand()` to return `true`
     - If command has non-deterministic behavior, override `getReplicationArgs()` to return canonical arguments
     - If command should replicate as different command, override `getReplicationCommandName()`

5. **Testing:**
   - Always run `mvn test` before committing
   - Add JUnit 5 tests for new commands
   - Use Mockito for mocking dependencies
   - Integration tests can use the `test.sh` script (requires running server)

### Configuration

Server configuration is in `src/main/java/com/redis/config/RedisConfig.java` with defaults:
- `redis.port`: Default 6379
- `redis.boss.threads`: Boss thread pool size (default 1)
- `redis.worker.threads`: Worker thread pool size (default 1)
- `redis.cleanup.interval.ms`: Cleanup interval in ms (default 5000)
- `redis.expiry.enabled`: Enable TTL expiry (default true)

Configuration can be overridden by creating or modifying `src/main/resources/application.properties`.

## Common Issues and Workarounds

1. **Java Version Mismatch:** The project requires Java 25 but may fail to build on Java 17 with "invalid target release: 25" error. Solutions:
   - Install and use Java 25 (recommended)
   - Temporarily modify `pom.xml` to use available Java version: update the properties section (lines 12-15) and the maven-compiler-plugin configuration (lines 68-69, the `<source>` and `<target>` elements) for development testing
   - Note: The `--enable-preview` flag is configured in both compiler and runtime settings

2. **Missing Dependencies:** Always run `mvn clean install` after pulling changes to ensure all dependencies are up to date.

3. **Port Already in Use:** If port 6379 is in use, stop any running Redis instances or change the port in `RedisConfig.java`.

4. **Test Failures:** Some tests may fail if the server is already running. Ensure no other instance is bound to port 6379.

## Validation Steps

Before submitting changes:
1. Run `mvn clean install` - should complete without errors
2. Run `mvn test` - all tests should pass
3. Start the server with `java --enable-preview -jar target/redis-server.jar`
4. Test basic commands using `redis-cli` or the test script
5. Review code for thread safety if modifying storage layer

## Additional Notes

- **No CI/CD Pipeline:** Currently, there are no GitHub Actions workflows. Validation is manual.
- **Minimal Dependencies:** The project intentionally keeps dependencies minimal (Netty, JUnit, Mockito only).
- **Entry Point:** Main class is `com.redis.server.NettyRedisServer`
- **Executable JAR:** Built using Maven Shade plugin, producing `redis-server.jar` in `target/`

---

**Trust these instructions:** This information has been validated against the current codebase. Only search for additional details if these instructions are incomplete or incorrect.
