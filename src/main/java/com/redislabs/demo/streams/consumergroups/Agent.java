package com.redislabs.demo.streams.consumergroups;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public abstract class Agent implements Runnable, Closeable {

    private static Logger logger = Logger.getLogger(Agent.class.getName());

    static final String REDIS_URL = "redis://localhost:6379";

    static final String STREAM_NAME = "aStream";

    static final Random RANDOM = new Random();

    RedisClient redisClient;

    StatefulRedisConnection<String, String> connection;

    RedisCommands<String, String> syncCommands;

    ThreadPoolExecutor executor;

    static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    Agent(String url) {
        redisClient = RedisClient.create(url);
        connection = redisClient.connect();
        syncCommands = connection.sync();
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    }

    static String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int)(RANDOM.nextDouble()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

    static int getRandomInt(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return RANDOM.nextInt((max - min) + 1) + min;
    }

    static void printUsage() {
        logger.info("USAGE: Agent [produce | consume group size]");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];

        if (command.equals("produce")) {
            Producer producer = new Producer(REDIS_URL);
            producer.executor.submit(producer);

        } else if (command.equals("consume")) {
            String group = args[1];
            int size = Integer.valueOf(args[2]);

            for(int i = 0; i < size; i++) {
                Consumer consumer = new Consumer(REDIS_URL, group);
                consumer.executor.submit(consumer);
            }

        } else {
            printUsage();
            System.exit(1);

        }
    }

    @Override
    public void close() throws IOException {
        connection.close();
        redisClient.shutdown();
        executor.shutdown();
    }

}
