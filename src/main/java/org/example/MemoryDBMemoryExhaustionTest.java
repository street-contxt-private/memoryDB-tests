package org.example;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryDBMemoryExhaustionTest {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;
    private static final int VALUE_SIZE_KB = 100;
    private static final int BATCH_SIZE = 1000;
    private static final int VERIFY_BATCH_SIZE = 100;
    private static final AtomicLong totalBytesWritten = new AtomicLong(0);

    public static void main(String[] args) {
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort(CLUSTER_ENDPOINT, PORT));

        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        try (JedisCluster jedis = new JedisCluster(clusterNodes, clientConfig)) {
            System.out.println("Starting memory exhaustion test...");
            System.out.println("Target instance: db.r6gd.xlarge (26.32 GiB memory)");

            writeAndVerifyInBatches(jedis);
        }
    }

    private static void writeAndVerifyInBatches(JedisCluster jedis) {
        byte[] valueData = new byte[VALUE_SIZE_KB * 1024];
        new Random().nextBytes(valueData);
        String baseValue = Base64.getEncoder().encodeToString(valueData);

        long keyCounter = 0;
        int consecutiveFailures = 0;
        List<String> currentBatchKeys = new ArrayList<>();

        while (consecutiveFailures < 3) {
            try {
                // Write batch
                currentBatchKeys.clear();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    String key = "test-key-" + keyCounter++;
                    String value = baseValue + UUID.randomUUID();
                    jedis.setex(key, 3600, value);
                    currentBatchKeys.add(key);
                    totalBytesWritten.addAndGet(value.length());
                }

                // Verify batch immediately
                verifyBatch(jedis, currentBatchKeys, baseValue);

                double gbWritten = totalBytesWritten.get() / (1024.0 * 1024.0 * 1024.0);
                System.out.printf("Written and verified %.2f GB (Keys: %d)%n", gbWritten, keyCounter);
                consecutiveFailures = 0;

            } catch (JedisDataException e) {
                consecutiveFailures++;
                System.out.println("Write failure detected: " + e.getMessage());
            } catch (Exception e) {
                System.err.println("Unexpected error: " + e.getMessage());
                break;
            }

            // Force GC after each batch
            System.gc();
        }

        System.out.println("\nFinal Results:");
        System.out.printf("Total Data Written: %.2f GB%n", totalBytesWritten.get() / (1024.0 * 1024.0 * 1024.0));
        System.out.printf("Total Keys: %d%n", keyCounter);
    }

    private static void verifyBatch(JedisCluster jedis, List<String> keys, String baseValue) {
        int verified = 0, missing = 0, mismatches = 0;

        for (int i = 0; i < keys.size(); i += VERIFY_BATCH_SIZE) {
            int end = Math.min(i + VERIFY_BATCH_SIZE, keys.size());
            List<String> batchToVerify = keys.subList(i, end);

            for (String key : batchToVerify) {
                String actualValue = jedis.get(key);
                if (actualValue == null) missing++;
                else if (!actualValue.startsWith(baseValue)) mismatches++;
                else verified++;
            }
        }

        if (missing > 0 || mismatches > 0) {
            System.out.printf("Batch verification: Verified=%d, Missing=%d, Mismatches=%d%n",
                verified, missing, mismatches);
        }
    }
}
