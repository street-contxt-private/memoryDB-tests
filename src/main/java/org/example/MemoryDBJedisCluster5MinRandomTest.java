package org.example;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MemoryDBJedisCluster5MinRandomTest {
    private static final int TOTAL_KEYS = 1000;
    private static final int TEST_DURATION_MINUTES = 1;
    private static final int OPS_PER_SECOND = 20;
    private static final int TOTAL_OPERATIONS = TEST_DURATION_MINUTES * 60 * OPS_PER_SECOND;
    private static final int TTL_30_MIN = 1800; // 30 minutes in seconds
    private static final int TTL_15_MIN = 900;  // 15 minutes in seconds

    public static void main(String[] args) {
        System.out.println("Starting MemoryDB JedisCluster 5-minute Random Test...");

        // MemoryDB cluster configuration
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort("clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com", 6379));

        // Configure Jedis client for MemoryDB (TLS enabled)
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        Random random = new Random();

        try (JedisCluster jedisCluster = new JedisCluster(clusterNodes, clientConfig)) {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + TimeUnit.MINUTES.toMillis(TEST_DURATION_MINUTES);

            for (int i = 0; i < TOTAL_OPERATIONS; i++) {
                // Choose a random key from 1000 available keys
                String key = "test-key-" + random.nextInt(TOTAL_KEYS);

                boolean isWrite = random.nextBoolean(); // 50% write, 50% read

                if (isWrite) {
                    boolean isUpdatingTTL = random.nextBoolean();

                    if (isUpdatingTTL && jedisCluster.exists(key)) {
                        // Update TTL to 15 minutes
                        jedisCluster.expire(key, TTL_15_MIN);
                        System.out.println("Updated TTL to 15 minutes for key: " + key);
                    } else {
                        // Generate a random value (10-100 chars)
                        String value = generateRandomString(10 + random.nextInt(91));
                        jedisCluster.setex(key, TTL_30_MIN, value); // Set new key with TTL 30 minutes
                        System.out.println("Set key: " + key + " | TTL: 30 minutes | Value Size: " + value.length());
                    }
                } else {
                    // Read operation
                    String value = jedisCluster.get(key);
                    System.out.println("Read key: " + key + " -> " + (value == null ? "NULL" : "Exists"));
                }

                // Sleep to maintain 5 ops per second
                Thread.sleep(1000 / OPS_PER_SECOND);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
