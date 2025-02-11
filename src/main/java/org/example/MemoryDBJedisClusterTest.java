package org.example;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import java.util.HashSet;
import java.util.Set;


public class MemoryDBJedisClusterTest {
    public static void main(String[] args) {
        // Create cluster connection configuration
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort(
            "clustercfg.memory-db-poc.gcxmaa.memorydb.us-east-1.amazonaws.com",
            6379
        ));

        // Configure Jedis client
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
            .ssl(true)
            .connectionTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .socketTimeoutMillis(Protocol.DEFAULT_TIMEOUT)
            .build();

        // Create cluster connection with SSL enabled
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, clientConfig)) {

            // Should be no MOVED exceptions
            String firstRead = jedisCluster.get("test-key-123456");
            System.out.println("First read test-key-123456: " + firstRead);

            // Set and get a key
            jedisCluster.set("test-key", "Hello from MemoryDB JedisCluster!");
            String value = jedisCluster.get("test-key");

            System.out.println("Stored value: " + value);

            // Test persistence
            System.out.println("Waiting 5 ms before checking key again...");
            Thread.sleep(5);
            System.out.println("Checking key after wait: " + jedisCluster.get("test-key"));

            // Delete the key
            jedisCluster.del("test-key");
            System.out.println("Deleted key: " + jedisCluster.get("test-key"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
