package com.redislabs.demo.streams;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Agent {

    static String STREAMS_NAME = "aStream";

    RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;
    RedisCommands<String, String> syncCommands;

    public Agent(String url) {
        redisClient = RedisClient.create(url);
        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    void produce(int nbOfMessageToSend) {

        for (int i = 0 ; i < nbOfMessageToSend ; i++) {

            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("speed", "15");
            messageBody.put("direction", "270");
            messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));
            messageBody.put("loop_info", String.valueOf( i ));

            String messageId = syncCommands.xadd(STREAMS_NAME, messageBody);

            System.out.println(String.format("\tMessage %s : %s posted", messageId, messageBody));
        }

        System.out.println("\n");

        connection.close();
        redisClient.shutdown();

    }


    void consume() {

        try {
            syncCommands.xgroupCreate(XReadArgs.StreamOffset.from(STREAMS_NAME, "0-0"), "application_1");
        } catch (RedisBusyException redisBusyException) {
            System.out.println(String.format("\t Group '%s' already exists", "application_1"));
        }


        System.out.println("Waiting for new messages");

        while (true) {

            List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                    io.lettuce.core.Consumer.from("application_1", "consumer_1"),
                    XReadArgs.StreamOffset.lastConsumed(STREAMS_NAME)
            );

            if (!messages.isEmpty()) {
                System.out.println(messages);
            }

        }

    }

    static void printUsage() {
        System.out.println("USAGE: Agent [produce [amount] | consume]");
    }

    public static void main(String[] args) {

        if (args == null || args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];
        Agent agent = new Agent("redis://localhost:6379");

        if (command.equals("produce")) {

            int amount = 1;

            if (args.length == 2) {
                amount = Integer.valueOf(args[1]);
            }

            System.out.println( String.format("\n Sending %s message(s)", amount));

            agent.produce(amount);

        } else if (command.equals("consume")) {

            agent.consume();

        } else {
            printUsage();
            System.exit(1);
        }

    }

}
