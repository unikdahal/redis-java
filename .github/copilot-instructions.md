# Copilot Instructions for Redis Java Server

## High-Level Overview

This repository contains a lightweight, high-performance, in-memory Redis-compatible server built with **Java 25** and **Netty**. The project implements a subset of Redis commands (SET, GET, DEL) using the Redis Serialization Protocol (RESP) for communication. It's designed for simplicity, performance, and educational purposes.

**Key Technologies:**
- Java 25 with preview features enabled
- Netty 4.1.118.Final (asynchronous, event-driven networking)
- Maven 3.9+ for build management
- JUnit 5 and Mockito for testing
- RESP (Redis Serialization Protocol) for client-server communication

**Important Note on Java Version:** The project is configured for Java 25 with preview features, but many development environments may have Java 17 or other versions installed. If you encounter "invalid target release: 25" errors, you have two options:
1. Install Java 25 and configure it as the active JDK
2. Temporarily adjust the `pom.xml` to use the available Java version (update `maven.compiler.source` and `maven.compiler.target` properties), keeping in mind this is for development/testing only

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
│   │   │   ├── SetCommand.java      # SET command implementation
│   │   │   ├── GetCommand.java      # GET command implementation
│   │   │   └── DelCommand.java      # DEL command implementation
│   │   ├── config/
│   │   │   └── RedisConfig.java     # Server configuration
│   │   ├── server/
│   │   │   ├── NettyRedisServer.java    # Main server class (entry point)
│   │   │   └── RedisCommandHandler.java # RESP protocol handler
│   │   ├── storage/
│   │   │   ├── RedisDatabase.java   # In-memory data store (ConcurrentHashMap)
│   │   │   └── ExpiryManager.java   # Key expiration manager (DelayQueue)
│   │   └── util/
│   │       └── ExpiryTask.java      # Expiry task implementation
│   ├── main/test/test.sh            # Manual integration test script
│   └── test/java/com/redis/
│       ├── commands/                # Unit tests for commands
│       │   ├── SetCommandTest.java
│       │   ├── GetCommandTest.java
│       │   ├── DelCommandTest.java
│       │   └── GetDelCommandTest.java
│       └── storage/                 # Unit tests for storage
│           └── RedisDatabaseTest.java
└── target/
    └── redis-server.jar             # Executable JAR (generated after build)
```

## Architecture

The server follows a layered architecture:

1. **Network Layer (Netty):** `NettyRedisServer` accepts connections using Netty's boss and worker thread pools
2. **Protocol Layer:** `RedisCommandHandler` parses RESP protocol and delegates to command registry
3. **Command Layer:** Individual command implementations (`SetCommand`, `GetCommand`, `DelCommand`) in `commands/`
4. **Storage Layer:** `RedisDatabase` manages in-memory key-value storage with `ConcurrentHashMap`
5. **Expiration Layer:** `ExpiryManager` handles TTL-based key expiration using `DelayQueue`

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

5. **Testing:**
   - Always run `mvn test` before committing
   - Add JUnit 5 tests for new commands
   - Use Mockito for mocking dependencies
   - Integration tests can use the `test.sh` script (requires running server)

### Configuration

Server configuration is in `src/main/java/com/redis/config/RedisConfig.java`:
- `redis.port`: Default 6379
- `redis.boss.threads`: Boss thread pool size (default 1)
- `redis.worker.threads`: Worker thread pool size (default 1)

Configuration can be overridden via `src/main/resources/application.properties` (if it exists).

## Common Issues and Workarounds

1. **Java Version Mismatch:** The project requires Java 25 but may fail to build on Java 17 with "invalid target release: 25" error. Solutions:
   - Install and use Java 25 (recommended)
   - Temporarily modify `pom.xml` to use available Java version (lines 12-13 and 68-69) for development testing
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
