package org.example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MemoryDBJedisTest {
    public static void main(String[] args) {
        String memoryDbHost = "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com";
        int port = 6379;

        // Create a connection pool
        JedisPool pool = new JedisPool(new JedisPoolConfig(), memoryDbHost, port, true);

        try (Jedis jedis = pool.getResource()) {
            // MOVED exception sometimes
            String firstRead = jedis.get("test-key-123456");
            System.out.println("First read: " + firstRead);

            // Set and get a key
            jedis.set("test-key", "Hello from MemoryDB Jedis!");
            String value = jedis.get("test-key");

            System.out.println("Stored value: " + value);

            // Test persistence
            System.out.println("Waiting 5 ms before checking key again...");
            Thread.sleep(5);
            System.out.println("Checking key after wait: " + jedis.get("test-key"));

            // Delete the key
            jedis.del("test-key");
            System.out.println("Deleted key: " + jedis.get("test-key"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.close();
        }
    }
}
