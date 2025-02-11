package org.example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MemoryDBJedis5MinRandomTest {
    private static final int TOTAL_KEYS = 1000;
    private static final int TEST_DURATION_MINUTES = 5;
    private static final int OPS_PER_SECOND = 5;
    private static final int TOTAL_OPERATIONS = TEST_DURATION_MINUTES * 60 * OPS_PER_SECOND;
    private static final int TTL_30_MIN = 1800; // 30 minutes in seconds
    private static final int TTL_15_MIN = 900;  // 15 minutes in seconds

    public static void main(String[] args) {
        System.out.println("Starting MemoryDB Jedis 5-minute Random Test...");

        String memoryDbHost = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
        int port = 6379;

        // Create a connection pool
        JedisPool pool = new JedisPool(new JedisPoolConfig(), memoryDbHost, port, true);
        Random random = new Random();

        try (Jedis jedis = pool.getResource()) {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + TimeUnit.MINUTES.toMillis(TEST_DURATION_MINUTES);

            for (int i = 0; i < TOTAL_OPERATIONS; i++) {
                // Choose a random key from 1000 available keys
                String key = "test-key-" + random.nextInt(TOTAL_KEYS);

                boolean isWrite = random.nextBoolean(); // 50% write, 50% read

                if (isWrite) {
                    boolean isUpdatingTTL = random.nextBoolean();

                    if (isUpdatingTTL && jedis.exists(key)) {
                        // Update TTL to 15 minutes
                        jedis.expire(key, TTL_15_MIN);
                        System.out.println("Updated TTL to 15 minutes for key: " + key);
                    } else {
                        // Generate a random value (10-100 chars)
                        String value = generateRandomString(10 + random.nextInt(91));
                        jedis.setex(key, TTL_30_MIN, value); // Set new key with TTL 30 minutes
                        System.out.println("Set key: " + key + " | TTL: 30 minutes | Value Size: " + value.length());
                    }
                } else {
                    // Read operation
                    String value = jedis.get(key);
                    System.out.println("Read key: " + key + " -> " + (value == null ? "NULL" : "Exists"));
                }

                // Sleep to maintain 5 ops per second
                Thread.sleep(1000 / OPS_PER_SECOND);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.close();
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
