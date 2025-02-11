package org.example;

import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryDBWriter {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;
    private static final int TOTAL_KEYS = 200_000;
    private static final int THREAD_COUNT = 5;
    private static final byte[] MB_DATA = new byte[1024 * 1024]; // 1 MB
    private static final AtomicInteger keysWritten = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort(CLUSTER_ENDPOINT, PORT));

        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        new Random().nextBytes(MB_DATA);
        String baseValue = Base64.getEncoder().encodeToString(MB_DATA);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < THREAD_COUNT; t++) {
            executor.submit(() -> {
                try (JedisCluster jedis = new JedisCluster(clusterNodes, clientConfig)) {
                    while (true) {
                        int currentKey = keysWritten.getAndIncrement();
                        if (currentKey >= TOTAL_KEYS) break;

                        String key = "key" + currentKey;
                        try {
                            jedis.set(key, baseValue + " " + key);

                            if (currentKey % 100 == 0) {
                                double gbWritten = ((long) currentKey * baseValue.length()) / (1024.0 * 1024.0 * 1024.0);
                                System.out.printf("Written %.2f GB (Keys: %d)%n", gbWritten, currentKey);
                            }
                        } catch (Exception e) {
                            System.err.println("Write failed at " + key + ": " + e.getMessage());
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
        System.out.printf("Write completed in %.2f seconds%n", (System.currentTimeMillis() - startTime)/1000.0);
    }
}
