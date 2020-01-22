package com.redislabs.demo.streams;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;

import java.util.List;

public class Consumer extends Agent {

    public Consumer(String url) {
        super(url);
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

}
