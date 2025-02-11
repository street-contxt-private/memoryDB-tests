package org.example;

import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class MemoryDBVerifier {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;
    private static final int TOTAL_KEYS = 1300;
    private static final int THREAD_COUNT = 1;
    private static final int BATCH_SIZE = 100;

    private static final AtomicInteger keysVerified = new AtomicInteger(0);
    private static final AtomicInteger verifiedCount = new AtomicInteger(0);
    private static final AtomicInteger missingCount = new AtomicInteger(0);
    private static final AtomicInteger invalidCount = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort(CLUSTER_ENDPOINT, PORT));

        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        System.out.println("Starting parallel verification with " + THREAD_COUNT + " threads...");

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < THREAD_COUNT; t++) {
            executor.submit(() -> {
                try (JedisCluster jedis = new JedisCluster(clusterNodes, clientConfig)) {
                    while (true) {
                        int startKey = keysVerified.getAndAdd(BATCH_SIZE);
                        if (startKey >= TOTAL_KEYS) break;
                        verifyBatch(jedis, startKey, Math.min(startKey + BATCH_SIZE, TOTAL_KEYS));
                    }
                } catch (Exception e) {
                    System.err.println("Got exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long duration = (System.currentTimeMillis() - startTime) / 1000;

        // Final results
        System.out.printf("%nFinal Results:%nVerified: %d%nMissing: %d%nInvalid: %d%nTime taken: %d seconds%n",
            verifiedCount.get(), missingCount.get(), invalidCount.get(), duration);
    }

    private static void verifyBatch(JedisCluster jedis, int start, int end) {
        for (int i = start; i < end; i++) {
            String key = "key" + i;
            String value = jedis.get(key);

            if (value == null) {
                missingCount.incrementAndGet();
            } else if (!value.endsWith(key)) {
                invalidCount.incrementAndGet();
            } else {
                verifiedCount.incrementAndGet();
            }
        }

        if (start % 100 == 0) { // Log progress every 100 keys
            System.out.printf("Progress: %d/%d, Verified: %d, Missing: %d, Invalid: %d%n",
                start, TOTAL_KEYS, verifiedCount.get(), missingCount.get(), invalidCount.get());
        }
    }
}
