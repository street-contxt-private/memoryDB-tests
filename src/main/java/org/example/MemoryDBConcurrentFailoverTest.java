package org.example;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Protocol;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryDBConcurrentFailoverTest {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;
    private static final int TOTAL_KEYS = 10000;
    private static final int CONCURRENT_WRITERS = 10;
    private static final int WRITE_DURATION_SECONDS = 60;

    private static final AtomicInteger successfulWrites = new AtomicInteger(0);
    private static final AtomicInteger failedWrites = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, String> writtenData = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Starting MemoryDB Concurrent Failover Persistence Test...");

        // MemoryDB cluster configuration
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort(CLUSTER_ENDPOINT, PORT));

        // Configure Jedis client for MemoryDB (TLS enabled)
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        // Concurrent write test
        ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_WRITERS);

        try (JedisCluster jedisCluster = new JedisCluster(clusterNodes, clientConfig)) {
            // Start concurrent writers
            for (int i = 0; i < CONCURRENT_WRITERS; i++) {
                executorService.submit(() -> concurrentWrite(jedisCluster));
            }

            // Allow writing for specified duration
            executorService.shutdown();
            executorService.awaitTermination(WRITE_DURATION_SECONDS, TimeUnit.SECONDS);

            // Verification phase
            System.out.println("\nVerification Phase:");
            verifyWrittenData(jedisCluster);
        } catch (Exception e) {
            System.err.println("Error during concurrent persistence test: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void concurrentWrite(JedisCluster jedisCluster) {
        Random random = new Random();
        long endTime = System.currentTimeMillis() + (WRITE_DURATION_SECONDS * 1000);

        while (System.currentTimeMillis() < endTime) {
            try {
                String key = "concurrent-test-" + UUID.randomUUID().toString();
                String value = "value-" + random.nextInt(1000000);

                jedisCluster.setex(key, 3600, value); // 1 hour TTL

                writtenData.put(key, value);
                successfulWrites.incrementAndGet();
            } catch (Exception e) {
                failedWrites.incrementAndGet();
            }
        }
    }

    private static void verifyWrittenData(JedisCluster jedisCluster) {
        int verified = 0;
        int mismatches = 0;
        int missing = 0;

        System.out.println("Total keys written: " + writtenData.size());
        System.out.println("Successful writes: " + successfulWrites.get());
        System.out.println("Failed writes: " + failedWrites.get());

        for (Map.Entry<String, String> entry : writtenData.entrySet()) {
            String actualValue = jedisCluster.get(entry.getKey());

            if (actualValue == null) {
                missing++;
                System.out.println("Missing key: " + entry.getKey());
            } else if (entry.getValue().equals(actualValue)) {
                verified++;
            } else {
                mismatches++;
                System.out.println("Mismatch for key: " + entry.getKey());
            }

            if ((verified + mismatches + missing) % 100 == 0) {
                System.out.println("Checked " + (verified + mismatches + missing) + " keys...");
            }
        }

        System.out.println("\nVerification complete:");
        System.out.println("Total keys verified: " + verified);
        System.out.println("Total mismatches: " + mismatches);
        System.out.println("Total missing: " + missing);
    }
}
