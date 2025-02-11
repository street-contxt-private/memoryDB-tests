package org.example;

import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryDBStressTest {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;
    private static final int TOTAL_KEYS = 1_000_000;
    private static final int THREAD_COUNT = 10;
    private static final byte[] ONE_MB_DATA = new byte[1024 * 1024];
    private static final AtomicInteger keysWritten = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort(CLUSTER_ENDPOINT, PORT));

        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        // Initialize 1MB data
        new Random().nextBytes(ONE_MB_DATA);
        String value = Base64.getEncoder().encodeToString(ONE_MB_DATA);

        // Start writer threads
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try (JedisCluster jedis = new JedisCluster(clusterNodes, clientConfig)) {
                    while (true) {
                        int currentKey = keysWritten.getAndIncrement();
                        if (currentKey >= TOTAL_KEYS) break;

                        try {
                            jedis.set("key" + currentKey, value);

                            if (currentKey % 1000 == 0) {
                                double gbWritten = ((long) currentKey * value.length()) / (1024.0 * 1024.0 * 1024.0);
                                System.out.printf("Written %.2f GB (Keys: %d)%n", gbWritten, currentKey);
                            }
                        } catch (Exception e) {
                            System.err.println("Write failed at key" + currentKey + ": " + e.getMessage());
                            break;
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("\nWrite phase completed in %.2f seconds%n", duration/1000.0);

        // Verify
        verifyKeys(new JedisCluster(clusterNodes, clientConfig));
    }

    private static void verifyKeys(JedisCluster jedis) {
        System.out.println("\nStarting verification...");
        int verified = 0;
        int missing = 0;

        for (int i = 0; i < TOTAL_KEYS; i++) {
            if (jedis.exists("key" + i)) {
                verified++;
            } else {
                missing++;
            }

            if (i % 10000 == 0) {
                System.out.printf("Verified: %d, Missing: %d%n", verified, missing);
            }
        }

        System.out.printf("\nFinal results:%nVerified: %d%nMissing: %d%n", verified, missing);
    }
}
