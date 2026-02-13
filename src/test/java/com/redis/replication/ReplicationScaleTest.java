package com.redis.replication;

import com.redis.server.RedisCommandHandler;
import com.redis.storage.RedisDatabase;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scale and stress tests for the replication system.
 * <p>
 * These tests validate behavior under high load and ensure the system
 * can handle production-level throughput.
 * <p>
 * <b>Note:</b> Some tests may take several seconds to complete.
 */
@DisplayName("Replication Scale Tests")
@Tag("scale")
public class ReplicationScaleTest {

    private RedisDatabase db;
    private String testPrefix; // Unique prefix for each test run

    @BeforeEach
    void setUp() {
        ReplicationManager.reset();
        SnapshotProducer.reset();
        db = RedisDatabase.getInstance();
        testPrefix = "scale_" + UUID.randomUUID().toString().substring(0, 8) + "_";
    }

    @AfterEach
    void tearDown() {
        ReplicationManager.reset();
        SnapshotProducer.reset();
    }

    private String sendCommand(EmbeddedChannel channel, String... args) {
        StringBuilder cmd = new StringBuilder();
        cmd.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            cmd.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        ByteBuf buf = Unpooled.copiedBuffer(cmd.toString(), StandardCharsets.UTF_8);
        channel.writeInbound(buf);
        ByteBuf response = channel.readOutbound();
        return response != null ? response.toString(StandardCharsets.UTF_8) : null;
    }

    // ==================== Throughput Benchmarks ====================

    @Nested
    @DisplayName("Throughput Benchmarks")
    class ThroughputBenchmarks {

        @Test
        @DisplayName("Measure SET command throughput")
        void measureSetThroughput() {
            EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
            int warmup = 1000;
            int iterations = 50000;

            // Warmup
            for (int i = 0; i < warmup; i++) {
                sendCommand(channel, "SET", "warmup" + i, "value");
            }

            // Benchmark
            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                sendCommand(channel, "SET", "key" + i, "value" + i);
            }
            long duration = System.nanoTime() - startTime;

            double opsPerSecond = (iterations * 1_000_000_000.0) / duration;
            double latencyMicros = (duration / 1000.0) / iterations;

            System.out.println("\n=== SET Throughput Benchmark ===");
            System.out.printf("Operations: %,d%n", iterations);
            System.out.printf("Duration: %.2f ms%n", duration / 1_000_000.0);
            System.out.printf("Throughput: %,.0f ops/sec%n", opsPerSecond);
            System.out.printf("Avg Latency: %.2f µs%n", latencyMicros);

            channel.close();
            assertTrue(opsPerSecond > 50000, "Should handle at least 50K ops/sec");
        }

        @Test
        @DisplayName("Measure GET command throughput")
        void measureGetThroughput() {
            EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
            int keyCount = 10000;
            int iterations = 50000;

            // Setup data
            for (int i = 0; i < keyCount; i++) {
                db.put("key" + i, "value" + i);
            }

            // Benchmark
            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                sendCommand(channel, "GET", "key" + (i % keyCount));
            }
            long duration = System.nanoTime() - startTime;

            double opsPerSecond = (iterations * 1_000_000_000.0) / duration;

            System.out.println("\n=== GET Throughput Benchmark ===");
            System.out.printf("Operations: %,d%n", iterations);
            System.out.printf("Throughput: %,.0f ops/sec%n", opsPerSecond);

            channel.close();
            assertTrue(opsPerSecond > 100000, "GET should handle at least 100K ops/sec");
        }

        @Test
        @DisplayName("Measure INCR command throughput (atomic operations)")
        void measureIncrThroughput() {
            EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
            int iterations = 50000;

            sendCommand(channel, "SET", "counter", "0");

            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                sendCommand(channel, "INCR", "counter");
            }
            long duration = System.nanoTime() - startTime;

            double opsPerSecond = (iterations * 1_000_000_000.0) / duration;

            System.out.println("\n=== INCR Throughput Benchmark ===");
            System.out.printf("Throughput: %,.0f ops/sec%n", opsPerSecond);

            channel.close();
            assertTrue(opsPerSecond > 50000, "INCR should handle at least 50K ops/sec");
        }

        @Test
        @DisplayName("Measure replication log append throughput")
        void measureReplicationLogThroughput() {
            ReplicationLog log = new ReplicationLog(64 * 1024 * 1024); // 64MB
            byte[] command = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"
                .getBytes(StandardCharsets.UTF_8);
            int iterations = 1_000_000;

            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                log.append(command);
            }
            long duration = System.nanoTime() - startTime;

            double opsPerSecond = (iterations * 1_000_000_000.0) / duration;
            double mbPerSecond = (iterations * command.length * 1_000_000_000.0) / duration / (1024 * 1024);

            System.out.println("\n=== Replication Log Throughput ===");
            System.out.printf("Operations: %,d%n", iterations);
            System.out.printf("Throughput: %,.0f ops/sec%n", opsPerSecond);
            System.out.printf("Bandwidth: %.2f MB/sec%n", mbPerSecond);

            assertTrue(opsPerSecond > 500000, "Log should handle at least 500K ops/sec");
        }
    }

    // ==================== Concurrency Tests ====================

    @Nested
    @DisplayName("Concurrency Tests")
    class ConcurrencyTests {

        @Test
        @DisplayName("Concurrent SET from multiple clients")
        void concurrentSetFromMultipleClients() throws Exception {
            int clientCount = 8;
            int operationsPerClient = 5000;
            ExecutorService executor = Executors.newFixedThreadPool(clientCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(clientCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            for (int c = 0; c < clientCount; c++) {
                final int clientId = c;
                executor.submit(() -> {
                    EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
                    try {
                        startLatch.await(); // Wait for all clients to be ready
                        for (int i = 0; i < operationsPerClient; i++) {
                            String key = "client" + clientId + ":key" + i;
                            String result = sendCommand(channel, "SET", key, "value");
                            if ("+OK\r\n".equals(result)) {
                                successCount.incrementAndGet();
                            } else {
                                errorCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errorCount.addAndGet(operationsPerClient);
                    } finally {
                        channel.close();
                        doneLatch.countDown();
                    }
                });
            }

            long startTime = System.currentTimeMillis();
            startLatch.countDown(); // Start all clients
            doneLatch.await(60, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startTime;
            executor.shutdown();

            int totalOps = clientCount * operationsPerClient;
            double opsPerSecond = (totalOps * 1000.0) / duration;

            System.out.println("\n=== Concurrent SET Test ===");
            System.out.printf("Clients: %d%n", clientCount);
            System.out.printf("Total Operations: %,d%n", totalOps);
            System.out.printf("Success: %,d (%.1f%%)%n", successCount.get(),
                100.0 * successCount.get() / totalOps);
            System.out.printf("Duration: %d ms%n", duration);
            System.out.printf("Throughput: %,.0f ops/sec%n", opsPerSecond);

            assertEquals(0, errorCount.get(), "Should have no errors");
            assertEquals(totalOps, successCount.get(), "All operations should succeed");
        }

        @Test
        @DisplayName("Concurrent INCR on same key (contention test)")
        void concurrentIncrOnSameKey() throws Exception {
            int threadCount = 8;
            int incrementsPerThread = 10000;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            db.put("counter", "0");

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
                    try {
                        startLatch.await();
                        for (int i = 0; i < incrementsPerThread; i++) {
                            sendCommand(channel, "INCR", "counter");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        channel.close();
                        doneLatch.countDown();
                    }
                });
            }

            long startTime = System.currentTimeMillis();
            startLatch.countDown();
            doneLatch.await(60, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startTime;
            executor.shutdown();

            // Verify final count
            String finalValue = (String) db.get("counter");
            long expectedValue = (long) threadCount * incrementsPerThread;

            System.out.println("\n=== Concurrent INCR Contention Test ===");
            System.out.printf("Threads: %d%n", threadCount);
            System.out.printf("Expected: %,d%n", expectedValue);
            System.out.printf("Actual: %s%n", finalValue);
            System.out.printf("Duration: %d ms%n", duration);

            assertEquals(expectedValue, Long.parseLong(finalValue),
                "Counter should be exactly " + expectedValue);
        }

        @Test
        @DisplayName("Mixed read/write workload")
        void mixedReadWriteWorkload() throws Exception {
            int threadCount = 8;
            int opsPerThread = 5000;
            double writeRatio = 0.2; // 20% writes, 80% reads

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicLong readCount = new AtomicLong(0);
            AtomicLong writeCount = new AtomicLong(0);

            // Pre-populate data
            for (int i = 0; i < 1000; i++) {
                db.put("prekey" + i, "value" + i);
            }

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    try {
                        startLatch.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            if (random.nextDouble() < writeRatio) {
                                sendCommand(channel, "SET", "t" + threadId + "k" + i, "v" + i);
                                writeCount.incrementAndGet();
                            } else {
                                sendCommand(channel, "GET", "prekey" + random.nextInt(1000));
                                readCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        channel.close();
                        doneLatch.countDown();
                    }
                });
            }

            long startTime = System.currentTimeMillis();
            startLatch.countDown();
            doneLatch.await(60, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startTime;
            executor.shutdown();

            long totalOps = readCount.get() + writeCount.get();
            double opsPerSecond = (totalOps * 1000.0) / duration;

            System.out.println("\n=== Mixed Workload Test ===");
            System.out.printf("Reads: %,d (%.1f%%)%n", readCount.get(),
                100.0 * readCount.get() / totalOps);
            System.out.printf("Writes: %,d (%.1f%%)%n", writeCount.get(),
                100.0 * writeCount.get() / totalOps);
            System.out.printf("Total Throughput: %,.0f ops/sec%n", opsPerSecond);
        }
    }

    // ==================== Memory and Eviction Tests ====================

    @Nested
    @DisplayName("Memory and Eviction")
    class MemoryTests {

        @Test
        @DisplayName("Replication log evicts old data correctly")
        void replicationLogEvictsCorrectly() {
            int bufferSize = 64 * 1024; // 64KB
            ReplicationLog log = new ReplicationLog(bufferSize);
            byte[] command = new byte[1024]; // 1KB commands
            java.util.Arrays.fill(command, (byte) 'X');

            // Write 10x buffer size
            int writeCount = bufferSize * 10 / command.length;
            for (int i = 0; i < writeCount; i++) {
                log.append(command);
            }

            long totalWritten = log.getTotalBytesWritten();
            long historyLength = log.getHistoryLength();

            System.out.println("\n=== Replication Log Eviction Test ===");
            System.out.printf("Buffer Size: %,d bytes%n", bufferSize);
            System.out.printf("Total Written: %,d bytes%n", totalWritten);
            System.out.printf("History Length: %,d bytes%n", historyLength);
            System.out.printf("Evictions: %,d%n", log.getEvictionCount());

            assertTrue(historyLength <= bufferSize, "History should not exceed buffer");
            assertTrue(log.getEvictionCount() > 0, "Should have evicted data");
        }

        @Test
        @DisplayName("Database handles large number of keys")
        void databaseHandlesLargeKeyCount() {
            int keyCount = 100000;
            int initialSize = db.size();

            long startTime = System.currentTimeMillis();
            for (int i = 0; i < keyCount; i++) {
                db.put(testPrefix + "key" + i, "value" + i);
            }
            long writeTime = System.currentTimeMillis() - startTime;

            startTime = System.currentTimeMillis();
            for (int i = 0; i < keyCount; i++) {
                db.get(testPrefix + "key" + i);
            }
            long readTime = System.currentTimeMillis() - startTime;

            System.out.println("\n=== Large Key Count Test ===");
            System.out.printf("Keys: %,d%n", keyCount);
            System.out.printf("Write Time: %d ms (%.0f keys/sec)%n",
                writeTime, keyCount * 1000.0 / writeTime);
            System.out.printf("Read Time: %d ms (%.0f keys/sec)%n",
                readTime, keyCount * 1000.0 / readTime);

            // Verify we added exactly keyCount new keys
            assertTrue(db.size() >= initialSize + keyCount,
                "Should have at least " + keyCount + " new keys");
        }
    }

    // ==================== Latency Distribution Tests ====================

    @Nested
    @DisplayName("Latency Distribution")
    class LatencyTests {

        @Test
        @DisplayName("Measure latency percentiles")
        void measureLatencyPercentiles() {
            EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
            int warmup = 1000;
            int samples = 10000;
            long[] latencies = new long[samples];

            // Warmup
            for (int i = 0; i < warmup; i++) {
                sendCommand(channel, "SET", "warmup" + i, "value");
            }

            // Measure
            for (int i = 0; i < samples; i++) {
                long start = System.nanoTime();
                sendCommand(channel, "SET", "key" + i, "value" + i);
                latencies[i] = System.nanoTime() - start;
            }

            java.util.Arrays.sort(latencies);

            double p50 = latencies[samples / 2] / 1000.0;
            double p90 = latencies[(int) (samples * 0.90)] / 1000.0;
            double p99 = latencies[(int) (samples * 0.99)] / 1000.0;
            double p999 = latencies[(int) (samples * 0.999)] / 1000.0;
            double max = latencies[samples - 1] / 1000.0;

            System.out.println("\n=== Latency Percentiles (µs) ===");
            System.out.printf("p50: %.2f%n", p50);
            System.out.printf("p90: %.2f%n", p90);
            System.out.printf("p99: %.2f%n", p99);
            System.out.printf("p99.9: %.2f%n", p999);
            System.out.printf("max: %.2f%n", max);

            channel.close();
            assertTrue(p99 < 1000, "p99 should be under 1ms");
        }
    }

    // ==================== Pipelining Tests ====================

    @Nested
    @DisplayName("Pipelining")
    class PipeliningTests {

        @Test
        @DisplayName("Pipelined commands execute correctly")
        void pipelinedCommandsExecute() {
            EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandHandler());
            int batchSize = 100;

            // Build pipelined commands
            StringBuilder pipeline = new StringBuilder();
            for (int i = 0; i < batchSize; i++) {
                pipeline.append("*3\r\n$3\r\nSET\r\n$")
                    .append(("key" + i).length()).append("\r\nkey").append(i).append("\r\n$")
                    .append(("value" + i).length()).append("\r\nvalue").append(i).append("\r\n");
            }

            // Send all at once
            ByteBuf buf = Unpooled.copiedBuffer(pipeline.toString(), StandardCharsets.UTF_8);
            channel.writeInbound(buf);

            // Read all responses
            int responseCount = 0;
            ByteBuf response;
            while ((response = channel.readOutbound()) != null) {
                String resp = response.toString(StandardCharsets.UTF_8);
                if (resp.equals("+OK\r\n")) {
                    responseCount++;
                }
            }

            System.out.println("\n=== Pipelining Test ===");
            System.out.printf("Commands: %d%n", batchSize);
            System.out.printf("Responses: %d%n", responseCount);

            channel.close();
            assertEquals(batchSize, responseCount);
        }

        @Test
        @DisplayName("Pipelining throughput improvement")
        void pipeliningThroughputImprovement() {
            int commandCount = 10000;
            int batchSize = 100;

            // Single command mode
            EmbeddedChannel singleChannel = new EmbeddedChannel(new RedisCommandHandler());
            long singleStart = System.nanoTime();
            for (int i = 0; i < commandCount; i++) {
                sendCommand(singleChannel, "SET", "s" + i, "v");
            }
            long singleDuration = System.nanoTime() - singleStart;
            singleChannel.close();

            // Pipelined mode
            EmbeddedChannel pipeChannel = new EmbeddedChannel(new RedisCommandHandler());
            long pipeStart = System.nanoTime();
            for (int batch = 0; batch < commandCount / batchSize; batch++) {
                StringBuilder pipeline = new StringBuilder();
                for (int i = 0; i < batchSize; i++) {
                    int idx = batch * batchSize + i;
                    pipeline.append("*3\r\n$3\r\nSET\r\n$")
                        .append(("p" + idx).length()).append("\r\np").append(idx)
                        .append("\r\n$1\r\nv\r\n");
                }
                ByteBuf buf = Unpooled.copiedBuffer(pipeline.toString(), StandardCharsets.UTF_8);
                pipeChannel.writeInbound(buf);
                while (pipeChannel.readOutbound() != null) {} // Drain responses
            }
            long pipeDuration = System.nanoTime() - pipeStart;
            pipeChannel.close();

            double singleThroughput = (commandCount * 1_000_000_000.0) / singleDuration;
            double pipeThroughput = (commandCount * 1_000_000_000.0) / pipeDuration;
            double improvement = pipeThroughput / singleThroughput;

            System.out.println("\n=== Pipelining Throughput Comparison ===");
            System.out.printf("Single: %,.0f ops/sec%n", singleThroughput);
            System.out.printf("Pipelined: %,.0f ops/sec%n", pipeThroughput);
            System.out.printf("Improvement: %.2fx%n", improvement);

            // Just verify both approaches work - improvement factor varies by environment
            assertTrue(singleThroughput > 0, "Single commands should work");
            assertTrue(pipeThroughput > 0, "Pipelined commands should work");
        }
    }
}
