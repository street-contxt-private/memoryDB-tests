package org.example;

import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class MemoryDBRedissonTest {
    public static void main(String[] args) {
        String memoryDbHost = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
        int port = 6379;

        // Configure Redisson client
        Config config = new Config();
        config.useClusterServers()
            .addNodeAddress("rediss://" + memoryDbHost + ":" + port) // Enable SSL with "rediss://"
            .setRetryAttempts(5) // Handle failovers and retries
            .setRetryInterval(1500); // Time between retries

        RedissonClient redisson = Redisson.create(config);

        try {
            // Should be no MOVED exceptions
            RBucket<String> firstReadBucket = redisson.getBucket("test-key-123456");
            System.out.println("First read test-key-123456: " + firstReadBucket.get());

            // Set and get a key
            RBucket<String> bucket = redisson.getBucket("test-key");
            bucket.set("Hello from MemoryDB Redisson!");
            System.out.println("Stored value: " + bucket.get());

            // Test persistence
            System.out.println("Waiting 5 ms before checking key again...");
            Thread.sleep(5);
            System.out.println("Checking key after wait: " + bucket.get());

            // Delete the key
            bucket.delete();
            System.out.println("Deleted key: " + bucket.get());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redisson.shutdown();
        }
    }
}
