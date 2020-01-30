package com.redislabs.demo.streams.consumergroups;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;

public abstract class Agent implements Runnable, Closeable {

    static final String REDIS_URL = "redis://localhost:6379";

    static final String STREAM_NAME = "aStream";

    static final Random RANDOM = new Random();

    static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    RedisClient redisClient;

    StatefulRedisConnection<String, String> connection;

    RedisCommands<String, String> syncCommands;

    ThreadPoolExecutor executor;

    Agent(String url) {
        redisClient = RedisClient.create(url);
        connection = redisClient.connect();
        syncCommands = connection.sync();
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    }

    /**
     * Get a pseudorandom alpha-numeric string.
     *
     * @param count The length of the string.
     * @return The string.
     */
    static String getRandomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int)(RANDOM.nextDouble()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }

        return builder.toString();
    }

    /**
     * Get a pseudorandom integer.
     *
     * @param min Lower boundary of the integer.
     * @param max Upper boundary of the integer.
     * @return The integer.
     */
    static int getRandomInt(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return RANDOM.nextInt((max - min) + 1) + min;
    }

    @Override
    public void close() throws IOException {
        connection.close();
        redisClient.shutdown();
        executor.shutdown();
    }

}
