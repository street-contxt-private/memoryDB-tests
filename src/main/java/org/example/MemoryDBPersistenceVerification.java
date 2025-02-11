package org.example;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Protocol;

import java.util.*;
import java.nio.file.*;

public class MemoryDBPersistenceVerification {
    private static final String CLUSTER_ENDPOINT = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
    private static final int PORT = 6379;

    public static void main(String[] args) {
        System.out.println("Starting MemoryDB Persistence Verification...");

        // Load test data
        Map<String, String> expectedData = loadTestData();
        if (expectedData.isEmpty()) {
            System.out.println("No test data found. Run the persistence test first.");
            return;
        }

        // MemoryDB cluster configuration
        Set<HostAndPort> clusterNodes = new HashSet<>();
        clusterNodes.add(new HostAndPort(CLUSTER_ENDPOINT, PORT));

        // Configure Jedis client for MemoryDB (TLS enabled)
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        // Verify data after failure/restart
        try (JedisCluster jedisCluster = new JedisCluster(clusterNodes, clientConfig)) {
            System.out.println("Verifying data persistence...");
            verifyData(jedisCluster, expectedData);
        } catch (Exception e) {
            System.err.println("Error during persistence verification: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<String, String> loadTestData() {
        Map<String, String> data = new HashMap<>();
        try {
            List<String> lines = Files.readAllLines(Paths.get("persistence-test-data.txt"));
            for (String line : lines) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    data.put(parts[0], parts[1]);
                }
            }
        } catch (Exception e) {
            System.err.println("Error loading test data: " + e.getMessage());
        }
        return data;
    }

    private static void verifyData(JedisCluster jedisCluster, Map<String, String> expectedData) {
        int verified = 0;
        int mismatches = 0;
        int missing = 0;

        for (Map.Entry<String, String> entry : expectedData.entrySet()) {
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
