package org.example;

import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryDBWriterTimer {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;
    private static final int THREAD_COUNT = 100;
    private static final byte[] MB_DATA = new byte[1024]; // 1 kB
    private static final AtomicInteger keysWritten = new AtomicInteger(0);
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);
    private static final long RUN_TIME_MINUTES = 5;

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
        long endTime = startTime + (RUN_TIME_MINUTES * 60 * 1000);

        // Schedule the stop
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> {
            isRunning.set(false);
            System.out.println("Time's up! Stopping writers...");
        }, RUN_TIME_MINUTES, TimeUnit.MINUTES);

        // Start writer threads
        for (int t = 0; t < THREAD_COUNT; t++) {
            executor.submit(() -> {
                try (JedisCluster jedis = new JedisCluster(clusterNodes, clientConfig)) {
                    while (isRunning.get()) {
                        int currentKey = keysWritten.getAndIncrement();
                        String key = "key" + currentKey;

                        try {
                            jedis.set(key, baseValue + " " + key);

                            if (currentKey % 100 == 0) {
                                long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
                                double gbWritten = ((long) currentKey * baseValue.length()) / (1024.0 * 1024.0 * 1024.0);
                                System.out.printf("[%dm %ds] Written %.2f GB (Keys: %d)%n",
                                    elapsedSeconds / 60, elapsedSeconds % 60,
                                    gbWritten, currentKey);
                            }
                        } catch (Exception e) {
                            System.err.println("Write failed at " + key + ": " + e.getMessage());
                            isRunning.set(false);
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
        scheduler.shutdown();

        double totalTimeSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
        int totalKeys = keysWritten.get();
        double gbWritten = ((long) totalKeys * baseValue.length()) / (1024.0 * 1024.0 * 1024.0);

        System.out.println("\nFinal Results:");
        System.out.printf("Total time: %.2f seconds%n", totalTimeSeconds);
        System.out.printf("Total keys written: %d%n", totalKeys);
        System.out.printf("Total data written: %.2f GB%n", gbWritten);
        System.out.printf("Write rate: %.2f keys/second%n", totalKeys / totalTimeSeconds);
        System.out.printf("Write rate: %.2f MB/second%n", (gbWritten * 1024) / totalTimeSeconds);
    }
}
