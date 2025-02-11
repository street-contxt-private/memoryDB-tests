package org.example;

import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MemoryDBRedisson5MinRandomTest {
    private static final int TOTAL_KEYS = 1000;
    private static final int TEST_DURATION_MINUTES = 1;
    private static final int OPS_PER_SECOND = 20;
    private static final int TOTAL_OPERATIONS = TEST_DURATION_MINUTES * 60 * OPS_PER_SECOND;
    private static final int TTL_30_MIN = 1800; // 30 minutes in seconds
    private static final int TTL_15_MIN = 900;  // 15 minutes in seconds

    public static void main(String[] args) {
        System.out.println("Starting MemoryDB Redisson 5-minute Random Test...");

        String memoryDbHost = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
        int port = 6379;

        // Redisson Configuration for MemoryDB with TLS
        Config config = new Config();
        config.useClusterServers()
            .addNodeAddress("rediss://" + memoryDbHost + ":" + port) // Enable SSL with "rediss://"
            .setRetryAttempts(5) // Handle failovers and retries
            .setRetryInterval(1500); // Time between retries

        RedissonClient redisson = Redisson.create(config);
        Random random = new Random();

        try {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + TimeUnit.MINUTES.toMillis(TEST_DURATION_MINUTES);

            for (int i = 0; i < TOTAL_OPERATIONS; i++) {
                // Choose a random key from 1000 available keys
                String key = "test-key-" + random.nextInt(TOTAL_KEYS);
                RBucket<String> bucket = redisson.getBucket(key);

                boolean isWrite = random.nextBoolean(); // 50% write, 50% read

                if (isWrite) {
                    boolean isUpdatingTTL = random.nextBoolean();

                    if (isUpdatingTTL && bucket.isExists()) {
                        // Update TTL to 15 minutes
                        bucket.expire(TTL_15_MIN, TimeUnit.SECONDS);
                        System.out.println("Updated TTL to 15 minutes for key: " + key);
                    } else {
                        // Generate a random value (10-100 chars)
                        String value = generateRandomString(10 + random.nextInt(91));
                        bucket.set(value, TTL_30_MIN, TimeUnit.SECONDS); // Set new key with TTL 30 minutes
                        System.out.println("Set key: " + key + " | TTL: 30 minutes | Value Size: " + value.length());
                    }
                } else {
                    // Read operation
                    String value = bucket.get();
                    System.out.println("Read key: " + key + " -> " + (value == null ? "NULL" : "Exists"));
                }

                // Sleep to maintain 5 ops per second
                Thread.sleep(1000 / OPS_PER_SECOND);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redisson.shutdown();
        }

        System.out.println("Test completed after 5 minutes.");
    }

    // Helper function to generate random string of given length
    private static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }
}
