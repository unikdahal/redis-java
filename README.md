# 🚀 Java Redis Server

[![Java Version](https://img.shields.io/badge/Java-25-orange.svg)](https://openjdk.java.net/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Java CI](https://github.com/unikdahal/redis-java/actions/workflows/ci.yml/badge.svg)](https://github.com/unikdahal/redis-java/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/Tests-599%2B-brightgreen.svg)](#-testing)

A high-performance, lightweight, in-memory Redis-compatible server built from the ground up using **Java 25** and **Netty**. This project implements core Redis functionality with a focus on low latency, efficient memory usage, clean architecture, and production-grade replication.

---

## ✨ Features

### Core Capabilities
- **⚡ High Performance**: Built on Netty's asynchronous event-driven framework for massive concurrency
- **💾 In-Memory Storage**: Optimized data structures using `ConcurrentHashMap` for thread-safe, lock-free reads
- **🔌 Redis Protocol (RESP)**: Full RESP implementation, compatible with any standard Redis client (`redis-cli`, `jedis`, `lettuce`, `redis-py`, etc.)
- **⏳ Advanced Expiration**: Dual-strategy expiration (Lazy + Active background cleanup via `DelayQueue`)
- **🔄 Transaction Support**: Full MULTI/EXEC/DISCARD support with optimized batch execution
- **📊 Pipelining**: Full support for command pipelining with efficient batch processing

### Replication (Master-Replica)
- **🔗 Master-Replica Architecture**: Production-grade single-writer, log-based state machine
- **📡 Command Propagation**: Automatic propagation with backpressure handling
- **🔄 Deterministic Replication**: Commands are canonicalized before replication to ensure consistent state:
  - `XADD stream *` → `XADD stream <actual-id>` (auto-generated IDs resolved)
  - `SET key value EX 60` → `SET key value PXAT <timestamp>` (relative → absolute time)
  - `EXPIRE key 60` → `PEXPIREAT key <timestamp>` (relative → absolute time)
- **📦 Partial Resync (PSYNC2)**: Efficient reconnection with backlog-based partial resync
- **💾 RDB Snapshots**: Point-in-time snapshots for full resync
- **🚫 Write Protection**: Replicas automatically reject write commands (READONLY error)
- **📊 Replication Log**: Lock-free ring buffer with configurable size (64KB - 512MB)

### Advanced Features
- **🌊 Redis Streams**: Full XADD/XRANGE/XREAD support with blocking reads
- **🚫 Blocking Operations**: BLPOP with efficient async handling (no thread blocking)
- **🏗️ Extensible Command Registry**: Easy to add new commands via simple interface

### 🚀 Key Differentiators from Redis

| Feature | Redis | This Implementation |
|:--------|:------|:--------------------|
| **Offset Tracking** | Mutex-protected long | Lock-free `AtomicLong` |
| **Statistics** | Atomic increments | `LongAdder` (10x faster under contention) |
| **Backlog** | Single producer lock | Lock-free ring buffer with minimal contention |
| **Propagation** | Synchronous per-replica | Async with circuit breaker pattern |
| **WAIT Polling** | Fixed interval | Adaptive exponential backoff |
| **Memory** | Unbounded backlog growth | Bounded with automatic eviction |
| **Failure Handling** | Simple disconnect | Circuit breaker + self-healing recovery |
| **Health Monitoring** | Basic lag tracking | Per-replica health scores & backpressure detection |

---

## 🛠️ Supported Commands

Detailed documentation for each command can be found in the [docs/commands](./docs/commands) directory.

### 🔑 Connection & Utility
| Command | Usage | Description |
|:--------|:------|:------------|
| `PING` | `PING [message]` | Test connection, returns PONG or echoes message |
| `ECHO` | `ECHO message` | Echo the given message |
| `EXPIRE` | `EXPIRE key seconds` | Set key expiration in seconds |
| `TTL` | `TTL key` | Get remaining time to live in seconds |
| `TYPE` | `TYPE key` | Get the type of value stored at key |
| `DEL` | `DEL key [key ...]` | Delete one or more keys |

### 📝 String Operations
| Command | Usage | Description |
|:--------|:------|:------------|
| `SET` | `SET key value [EX s] [PX ms] [PXAT ms] [NX\|XX]` | Set string value with optional expiry |
| `GET` | `GET key` | Get the value of a key |
| `INCR` | `INCR key` | Increment integer value by 1 |

### 🔄 Transactions
| Command | Usage | Description |
|:--------|:------|:------------|
| `MULTI` | `MULTI` | Start a transaction block |
| `EXEC` | `EXEC` | Execute all commands in transaction |
| `DISCARD` | `DISCARD` | Discard all commands in transaction |

### 📋 List Operations
| Command | Usage | Description |
|:--------|:------|:------------|
| `LPUSH` | `LPUSH key element [element ...]` | Push elements to head of list |
| `RPUSH` | `RPUSH key element [element ...]` | Push elements to tail of list |
| `LPOP` | `LPOP key [count]` | Pop elements from head of list |
| `LLEN` | `LLEN key` | Get list length |
| `LRANGE` | `LRANGE key start stop` | Get range of elements |
| `BLPOP` | `BLPOP key [key ...] timeout` | Blocking pop from head |

### 🌊 Stream Operations
| Command | Usage | Description |
|:--------|:------|:------------|
| `XADD` | `XADD key ID field value [field value ...]` | Append entry to stream |
| `XRANGE` | `XRANGE key start end [COUNT count]` | Get range of entries |
| `XREAD` | `XREAD [COUNT c] [BLOCK ms] STREAMS key [key ...] id [id ...]` | Read from streams |

### 🔗 Replication Commands
| Command | Usage | Description |
|:--------|:------|:------------|
| `REPLCONF` | `REPLCONF <option> <value>` | Configure replication |
| `PSYNC` | `PSYNC replicationid offset` | Initiate replication sync |
| `INFO` | `INFO [section]` | Get server information |

---

## 🚀 Getting Started

### Prerequisites
- **JDK 25** or higher (with `--enable-preview`)
- **Maven 3.9+**

### Build & Run

```bash
# Clone the repository
git clone <repository-url>
cd redis-java

# Build the project
mvn clean package

# Start as master (default)
java --enable-preview -jar target/redis-server.jar

# Start as replica
java --enable-preview -jar target/redis-server.jar --port 6380 --replicaof localhost 6379
```

The server listens on port **6379** by default.

### Connect with `redis-cli`

```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey "Hello, Redis!"
OK
127.0.0.1:6379> GET mykey
"Hello, Redis!"
127.0.0.1:6379> SET counter 100
OK
127.0.0.1:6379> INCR counter
(integer) 101
```

### Replication Example

```bash
# Terminal 1: Start master
java --enable-preview -jar target/redis-server.jar --port 6379

# Terminal 2: Start replica
java --enable-preview -jar target/redis-server.jar --port 6380 --replicaof localhost 6379

# Terminal 3: Write to master
redis-cli -p 6379 SET foo bar
OK

# Terminal 4: Read from replica (automatically replicated!)
redis-cli -p 6380 GET foo
"bar"
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Connections                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Netty EventLoop (Boss + Worker)                      │
│  • Async I/O multiplexing       • Zero-copy buffer management               │
│  • Connection handling          • Backpressure support                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           RedisCommandHandler                                │
│  • RESP protocol parsing        • Pipelining support                        │
│  • Transaction management       • Command routing                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                        ┌──────────────┴──────────────┐
                        ▼                              ▼
┌──────────────────────────────────┐  ┌──────────────────────────────────────┐
│        CommandRegistry           │  │        ReplicationManager             │
│  • O(1) command lookup           │  │  • Master/Replica role management    │
│  • Extensible registration       │  │  • Command propagation               │
└──────────────────────────────────┘  │  • Backlog for partial resync        │
                        │              │  • Lock-free offset tracking         │
                        ▼              └──────────────────────────────────────┘
┌──────────────────────────────────┐                    │
│      Command Implementations     │                    │
│  • String: SET, GET, INCR        │                    ▼
│  • List: LPUSH, RPUSH, BLPOP     │  ┌──────────────────────────────────────┐
│  • Stream: XADD, XRANGE, XREAD   │  │        CommandPropagator             │
│  • Transaction: MULTI, EXEC      │  │  • Deterministic command rewriting   │
└──────────────────────────────────┘  │  • XADD * → XADD <actual-id>         │
                        │              │  • SET EX → SET PXAT                 │
                        ▼              │  • EXPIRE → PEXPIREAT                │
┌──────────────────────────────────┐  └──────────────────────────────────────┘
│          RedisDatabase           │                    │
│  • ConcurrentHashMap storage     │                    ▼
│  • Atomic compute operations     │  ┌──────────────────────────────────────┐
│  • Type-safe RedisValue wrapper  │  │         Replica Connections          │
└──────────────────────────────────┘  │  • Async command propagation         │
                        │              │  • Backpressure handling             │
                        ▼              │  • Lag monitoring                    │
┌──────────────────────────────────┐  └──────────────────────────────────────┘
│          ExpiryManager           │
│  • DelayQueue-based scheduling   │
│  • Background cleanup thread     │
│  • O(log n) insertion            │
└──────────────────────────────────┘
```

### Key Design Decisions

| Component | Design Choice | Rationale |
|:----------|:--------------|:----------|
| **Storage** | `ConcurrentHashMap` | Lock-free reads, segment-level locking for writes |
| **Lists** | `LinkedList` | O(1) head/tail operations for LPUSH/RPUSH/LPOP |
| **Streams** | `ConcurrentSkipListMap` | O(log n) range queries, thread-safe |
| **Expiry** | `DelayQueue` + lazy | Hybrid approach balances memory and CPU |
| **Replication** | Lock-free `AtomicLong` | High-throughput offset tracking |
| **Statistics** | `LongAdder` | 10x better than AtomicLong under contention |

---

## 🔄 Replication Deep Dive

### Architecture Overview

This implementation follows a **single-writer, log-based state machine** architecture with snapshot checkpoints and asynchronous followers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            REPLICATION ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                           MASTER                                     │   │
│   │  ┌──────────┐    ┌──────────────┐    ┌─────────────────────────┐   │   │
│   │  │ Accepts  │───▶│ Execute &    │───▶│ Append to               │   │   │
│   │  │ Writes   │    │ Canonicalize │    │ Replication Log         │   │   │
│   │  └──────────┘    └──────────────┘    └───────────┬─────────────┘   │   │
│   │                                                   │                  │   │
│   │                              ┌────────────────────┴──────────────┐  │   │
│   │                              ▼                                    ▼  │   │
│   │                   ┌──────────────────┐              ┌───────────────┐│   │
│   │                   │ Propagate to     │              │ Snapshot      ││   │
│   │                   │ Replicas         │              │ Producer      ││   │
│   │                   └──────────────────┘              └───────────────┘│   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│            ┌───────────────────────┼───────────────────────┐                │
│            ▼                       ▼                       ▼                │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │
│   │    REPLICA 1    │    │    REPLICA 2    │    │    REPLICA N    │        │
│   │  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │        │
│   │  │ Rejects   │  │    │  │ Rejects   │  │    │  │ Rejects   │  │        │
│   │  │ Writes    │  │    │  │ Writes    │  │    │  │ Writes    │  │        │
│   │  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │        │
│   │  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │        │
│   │  │ Replays   │  │    │  │ Replays   │  │    │  │ Replays   │  │        │
│   │  │ Commands  │  │    │  │ Commands  │  │    │  │ Commands  │  │        │
│   │  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │        │
│   └─────────────────┘    └─────────────────┘    └─────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Core Roles

| Role | Responsibilities | Behavior |
|:-----|:-----------------|:---------|
| **Master** | Accepts writes, executes commands, owns replication log | Propagates canonicalized commands to replicas |
| **Replica** | Rejects writes (READONLY), replays commands, tracks offset | Requests PSYNC for synchronization |
| **Snapshot Producer** | Creates point-in-time state snapshots | Independent of live mutations |

### Command Pipeline

The exact command execution flow ensures consistency:

```
parse → validate → canonicalize → execute → append to log → send to replicas
```

**Key principle:** Replication is always downstream, never upstream.

### Command Canonicalization

To ensure consistent state across master and replicas, commands are rewritten to their canonical (deterministic) form before propagation:

| Original Command | Propagated Command | Reason |
|:-----------------|:-------------------|:-------|
| `XADD stream * field value` | `XADD stream 1706745600000-0 field value` | Auto-generated IDs depend on timestamp |
| `SET key value EX 60` | `SET key value PXAT 1706745660000` | Relative time → absolute timestamp |
| `SET key value PX 5000` | `SET key value PXAT 1706745605000` | Relative time → absolute timestamp |
| `EXPIRE key 60` | `PEXPIREAT key 1706745660000` | Relative seconds → absolute milliseconds |

### Full Sync vs Partial Sync Decision

When a replica connects, the master uses this decision logic:

```
Replica connects → sends replicationId + offset
                          │
                          ▼
              ┌─────────────────────┐
              │ ID matches master?  │─── No ──▶ FULL SYNC
              └─────────────────────┘
                          │ Yes
                          ▼
              ┌─────────────────────┐
              │ Offset in backlog?  │─── No ──▶ FULL SYNC  
              └─────────────────────┘
                          │ Yes
                          ▼
                    PARTIAL SYNC
```

**No heuristics. No guessing.** The decision is deterministic.

---

## 🧪 Testing

We maintain high confidence through comprehensive unit and integration tests.

```bash
# Run all tests (Unit + Integration)
./run_all_tests.sh

# Run only unit tests
mvn test

# Run only integration tests  
mvn verify -DskipUnitTests

# Run specific test class
mvn test -Dtest=CommandPropagatorTest

# Run with verbose output
mvn test -X
```

### Test Coverage

| Category | Tests | Description |
|:---------|------:|:------------|
| Unit Tests | 400+ | Individual command and component testing |
| Integration Tests | 100+ | End-to-end RESP protocol testing |
| Replication Tests | 100+ | Master-replica sync, backlog, snapshots, scale tests |
| **Total** | **599** | Comprehensive coverage verified by CI |

### Replication Testing

```bash
# Run replication-specific tests
mvn test -Dtest=MasterReplicaIntegrationTest

# Run scale/stress tests (benchmarks)
mvn test -Dtest=ReplicationScaleTest

# Run end-to-end replication test with actual servers
./test_replication.sh

# Run with stress test
./test_replication.sh --stress

# Run with multiple replicas
./test_replication.sh --scale
```

---

## ⚙️ Configuration

### Command Line Arguments

```bash
java --enable-preview -jar target/redis-server.jar [options]

Options:
  --port <port>              Server port (default: 6379)
  --replicaof <host> <port>  Start as replica of specified master
```

### Configuration File

Edit `src/main/resources/application.properties`:

| Property | Default | Description |
|:---------|:--------|:------------|
| `redis.port` | 6379 | Port to listen on |
| `redis.boss.threads` | 1 | Number of acceptor threads |
| `redis.worker.threads` | 1 | Number of I/O threads |
| `redis.cleanup.interval.ms` | 5000 | Expiry cleanup interval |
| `redis.expiry.enabled` | true | Enable/disable key expiration |
| `redis.repl.backlog.size` | 1048576 | Replication backlog size (1MB) |

### Environment Variables

| Variable | Description |
|:---------|:------------|
| `REDIS_PORT` | Override server port |
| `REDIS_EXPIRY_ENABLED` | Override expiry setting |

---

## 🔒 Production Considerations

### Memory Management
- Keys are stored in a `ConcurrentHashMap` with lazy expiration
- Background cleanup prevents memory leaks from expired keys
- Replication backlog is bounded (1MB default) with automatic eviction
- Lists use `LinkedList` for O(1) push/pop operations
- Streams use `ConcurrentSkipListMap` for efficient range queries

### Thread Safety
- All storage operations are atomic via `compute()` operations
- Lock-free statistics using `LongAdder` (10x better than `AtomicLong` under contention)
- Per-connection state isolation (no shared mutable state between connections)
- Replication offsets tracked with `AtomicLong` for lock-free updates

### Monitoring
- `INFO` command provides comprehensive server statistics
- `INFO replication` shows per-replica lag, offset, and health
- Circuit breaker status and backpressure events tracked
- Enhanced metrics: `repl_commands_propagated`, `repl_bytes_propagated`, `repl_backpressure_events`

### High Availability
- Replicas automatically reject writes with `-READONLY` error
- Circuit breaker pattern protects against slow/failing replicas
- Automatic circuit breaker recovery after 5 seconds
- PSYNC2 enables efficient partial resync after brief disconnections

### Capacity Planning
| Workload | Backlog Size | Notes |
|:---------|:-------------|:------|
| Low (< 100 ops/s) | 64KB | Minimum for basic partial resync |
| Medium (< 10K ops/s) | 1MB (default) | Handles most workloads |
| High (> 10K ops/s) | 16-64MB | For write-heavy applications |
| Enterprise (> 100K ops/s) | 256-512MB | Maximum supported |

### Recommended JVM Settings

```bash
java --enable-preview \
  -Xms256m -Xmx1g \
  -XX:+UseZGC \
  -XX:+ZGenerational \
  -XX:+AlwaysPreTouch \
  -Djava.lang.Integer.IntegerCache.high=10000 \
  -jar target/redis-server.jar
```

### Production Checklist
- [ ] Set appropriate JVM heap size (`-Xmx`)
- [ ] Configure backlog size based on write volume
- [ ] Enable ZGC for low-latency GC pauses
- [ ] Monitor `INFO replication` for lag metrics
- [ ] Set up replica health alerts for circuit breaker trips

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Style
- Follow existing code patterns
- Add comprehensive JavaDoc for public APIs
- Include unit tests for new functionality
- Ensure all tests pass before submitting PR

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.

---

## 🙏 Acknowledgments

- [Redis](https://redis.io/) - The original inspiration
- [Netty](https://netty.io/) - High-performance networking framework
- [JUnit 5](https://junit.org/junit5/) - Testing framework
