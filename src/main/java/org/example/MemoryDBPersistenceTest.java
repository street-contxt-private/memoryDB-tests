package org.example;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Protocol;

import java.util.*;
import java.nio.file.*;

public class MemoryDBPersistenceTest {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;
    private static final int TEST_KEYS = 1000;

    public static void main(String[] args) {
        System.out.println("Starting MemoryDB Persistence Test...");

        // MemoryDB cluster configuration
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort(CLUSTER_ENDPOINT, PORT));

        // Configure Jedis client for MemoryDB (TLS enabled)
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        Random random = new Random();

        // Phase 1: Write initial test data
        try (JedisCluster jedisCluster = new JedisCluster(clusterNodes, clientConfig)) {
            System.out.println("Phase 1: Writing initial test data...");
            Map<String, String> writtenData = new HashMap<>();

            for (int i = 0; i < TEST_KEYS; i++) {
                String key = "persistence-test-key-" + i;
                String value = "test-value-" + UUID.randomUUID().toString();
                jedisCluster.setex(key, 3600, value); // 1 hour TTL
                writtenData.put(key, value);

                if (i % 100 == 0) {
                    System.out.println("Written " + i + " keys...");
                }
            }

            // Verify initial write
            System.out.println("\nVerifying initial data...");
            verifyData(jedisCluster, writtenData);

            // Save test data for later verification
            saveTestData(writtenData);
        } catch (Exception e) {
            System.err.println("Error during persistence test: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\n========================================");
        System.out.println("Initial data written and verified.");
        System.out.println("Next steps:");
        System.out.println("1. Use AWS Console to force a failover");
        System.out.println("2. Wait for cluster to stabilize");
        System.out.println("3. Run the verification script");
        System.out.println("========================================\n");
    }

    private static void verifyData(JedisCluster jedisCluster, Map<String, String> expectedData) {
        int verified = 0;
        int mismatches = 0;

        for (Map.Entry<String, String> entry : expectedData.entrySet()) {
            String actualValue = jedisCluster.get(entry.getKey());
            if (entry.getValue().equals(actualValue)) {
                verified++;
            } else {
                mismatches++;
                System.out.println("Mismatch for key: " + entry.getKey());
            }

            if ((verified + mismatches) % 100 == 0) {
                System.out.println("Verified " + (verified + mismatches) + " keys...");
            }
        }

        System.out.println("\nVerification complete:");
        System.out.println("Total keys verified: " + verified);
        System.out.println("Total mismatches: " + mismatches);
    }

    private static void saveTestData(Map<String, String> data) {
        try {
            Path file = Paths.get("persistence-test-data.txt");
            List<String> lines = new ArrayList<>();
            for (Map.Entry<String, String> entry : data.entrySet()) {
                lines.add(entry.getKey() + "," + entry.getValue());
            }
            Files.write(file, lines);
        } catch (Exception e) {
            System.err.println("Error saving test data: " + e.getMessage());
        }
    }
}
