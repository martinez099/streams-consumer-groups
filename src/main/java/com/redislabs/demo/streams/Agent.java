package com.redislabs.demo.streams;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.logging.Logger;

public class Agent {

    private static Logger logger = Logger.getLogger(Agent.class.getName());

    static final String REDIS_URL = "redis://localhost:6379";

    static final String STREAMS_NAME = "aStream";

    RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;
    RedisCommands<String, String> syncCommands;

    Agent(String url) {
        redisClient = RedisClient.create(url);
        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    static void printUsage() {
        logger.info("USAGE: Agent [produce amount | consume name group]");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];

        if (command.equals("produce")) {
            int amount = Integer.valueOf(args[1]);

            Producer producer = new Producer(REDIS_URL);
            producer.produce(amount);

            producer.connection.close();
            producer.redisClient.shutdown();

        } else if (command.equals("consume")) {
            String name = args[1];
            String group = args[2];

            Consumer consumer = new Consumer(REDIS_URL, group);
            consumer.consume(name);

            consumer.connection.close();
            consumer.redisClient.shutdown();

        } else {
            printUsage();
            System.exit(1);

        }

    }

}
