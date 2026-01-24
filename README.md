# Java Redis Server

A lightweight, high-performance, in-memory Redis-compatible server built with Java 25 and Netty. This project implements a subset of Redis commands and is designed for simplicity, performance, and educational purposes.

## Features

*   **In-Memory Storage**: Key-value store with optional time-to-live (TTL) support
*   **Netty-based Server**: Asynchronous, event-driven server for high performance
*   **Redis Protocol (RESP)**: Implements the Redis Serialization Protocol for communication
*   **Automatic Key Expiration**: Supports both lazy expiration and background cleanup via DelayQueue
*   **Thread-Safe Operations**: Uses `ConcurrentHashMap` for thread-safe data storage
*   **Supported Commands**:
    *   `SET key value [EX seconds] [PX milliseconds] [NX|XX]`
    *   `GET key`
    *   `DEL key [key ...]`

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    NettyRedisServer                         │
│  ┌─────────────┐    ┌─────────────────────────────────┐    │
│  │ Boss Group  │────│        Worker Group              │    │
│  │  (Accept)   │    │  ┌─────────────────────────────┐ │    │
│  └─────────────┘    │  │   RedisCommandHandler       │ │    │
│                     │  │   (RESP Parser/Executor)    │ │    │
│                     │  └─────────────────────────────┘ │    │
│                     └─────────────────────────────────┘    │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    CommandRegistry                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │SetCommand│  │GetCommand│  │DelCommand│                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     RedisDatabase                           │
│  ┌─────────────────────┐  ┌─────────────────────────────┐  │
│  │  ConcurrentHashMap  │  │      ExpiryManager          │  │
│  │   (Key → Value)     │  │  (DelayQueue-based cleanup) │  │
│  └─────────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
redis-java/
├── pom.xml                          # Maven build configuration
├── README.md                        # This file
├── src/
│   ├── main/java/com/redis/
│   │   ├── commands/                # Command implementations
│   │   │   ├── ICommand.java        # Command interface
│   │   │   ├── CommandRegistry.java # Command lookup registry
│   │   │   ├── SetCommand.java      # SET command
│   │   │   ├── GetCommand.java      # GET command
│   │   │   └── DelCommand.java      # DEL command
│   │   ├── config/
│   │   │   └── RedisConfig.java     # Server configuration
│   │   ├── server/
│   │   │   ├── NettyRedisServer.java    # Main server class
│   │   │   └── RedisCommandHandler.java # RESP protocol handler
│   │   ├── storage/
│   │   │   ├── RedisDatabase.java   # In-memory data store
│   │   │   └── ExpiryManager.java   # Key expiration manager
│   │   └── util/
│   │       └── ExpiryTask.java      # Expiry task implementation
│   └── test/java/com/redis/
│       ├── commands/                # Unit tests for commands
│       └── storage/                 # Unit tests for storage
└── target/
    └── redis-server.jar             # Executable JAR
```

## Getting Started

### Prerequisites

*   Java 25 or higher
*   Maven 3.9 or higher

### Building the Project

1.  Clone the repository:
    ```sh
    git clone https://github.com/your-username/redis-java.git
    ```
2.  Navigate to the project directory:
    ```sh
    cd redis-java
    ```
3.  Build the project using Maven:
    ```sh
    mvn clean install
    ```
    This will compile the code, run all tests, and create a runnable JAR file in the `target` directory.

### Running the Server

To start the Redis server, run the following command:

```sh
java --enable-preview -jar target/redis-server.jar
```

The server will start on the default port (6379).

## Usage

You can connect to the server using any Redis client, such as `redis-cli`.

### SET Command

Basic usage:
```
127.0.0.1:6379> SET mykey "Hello, Redis!"
OK
```

With expiration (EX = seconds, PX = milliseconds):
```
127.0.0.1:6379> SET mykey "expiring value" EX 10
OK
```

With conditional flags:
```
# NX - Only set if key does NOT exist
127.0.0.1:6379> SET mykey "value" NX
OK

# XX - Only set if key DOES exist
127.0.0.1:6379> SET mykey "new_value" XX
OK
```

### GET Command

```
127.0.0.1:6379> GET mykey
"Hello, Redis!"

# Non-existent key returns nil
127.0.0.1:6379> GET nonexistent
(nil)
```

### DEL Command

Delete single or multiple keys:
```
127.0.0.1:6379> SET key1 "value1"
OK
127.0.0.1:6379> SET key2 "value2"
OK
127.0.0.1:6379> DEL key1 key2
(integer) 2
127.0.0.1:6379> GET key1
(nil)
```

## Running Tests

To run the unit tests:

```sh
mvn test
```

To run tests with verbose output:

```sh
mvn test -X
```

## Configuration

The server can be configured via `src/main/resources/application.properties`:

| Property | Default | Description |
|----------|---------|-------------|
| `redis.port` | 6379 | Server port |
| `redis.boss.threads` | 1 | Boss thread pool size |
| `redis.worker.threads` | 1 | Worker thread pool size |

## Technical Details

### RESP Protocol

This server implements the Redis Serialization Protocol (RESP) for client-server communication:

- **Simple Strings**: `+OK\r\n`
- **Errors**: `-ERR message\r\n`
- **Integers**: `:1\r\n`
- **Bulk Strings**: `$6\r\nfoobar\r\n`
- **Null Bulk String**: `$-1\r\n`
- **Arrays**: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

### Key Expiration

Keys with TTL are expired using a dual approach:
1. **Lazy Expiration**: Keys are checked for expiry on access
2. **Active Expiration**: Background thread removes expired keys using a `DelayQueue`

## License

This project is open source and available under the MIT License.

