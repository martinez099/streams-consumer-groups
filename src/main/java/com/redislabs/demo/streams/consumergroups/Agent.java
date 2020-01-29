package com.redislabs.demo.streams.consumergroups;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.Random;
import java.util.logging.Logger;

public class Agent {

    private static Logger logger = Logger.getLogger(Agent.class.getName());

    static final String REDIS_URL = "redis://localhost:6379";

    static final String STREAM_NAME = "aStream";

    static final Random RANDOM = new Random();

    RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;
    RedisCommands<String, String> syncCommands;

    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

    Agent(String url) {
        redisClient = RedisClient.create(url);
        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    static int getRandomInt(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return RANDOM.nextInt((max - min) + 1) + min;
    }

    static void printUsage() {
        logger.info("USAGE: Agent [ produce | consume group ]");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];

        if (command.equals("produce")) {

            Producer producer = new Producer(REDIS_URL);
            try {
                producer.produce();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            producer.connection.close();
            producer.redisClient.shutdown();

        } else if (command.equals("consume")) {
            String group = args[1];

            Consumer consumer = new Consumer(REDIS_URL, group);
            try {
                consumer.consume();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            consumer.connection.close();
            consumer.redisClient.shutdown();

        } else {
            printUsage();
            System.exit(1);

        }

    }

}
