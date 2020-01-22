package com.redislabs.demo.streams;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;


public class Agent {

    static String REDIS_URL = "redis://localhost:6379";

    static String STREAMS_NAME = "aStream";

    RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;
    RedisCommands<String, String> syncCommands;

    Agent(String url) {
        redisClient = RedisClient.create(url);
        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    static void printUsage() {
        System.out.println("USAGE: Agent [produce [amount] | consume]");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];

        if (command.equals("produce")) {
            int amount = 1;
            if (args.length == 2) {
                amount = Integer.valueOf(args[1]);
            }

            Producer producer = new Producer(REDIS_URL);
            producer.produce(amount);

        } else if (command.equals("consume")) {
            Consumer consumer = new Consumer(REDIS_URL);
            consumer.consume();

        } else {
            printUsage();
            System.exit(1);

        }

    }

}
