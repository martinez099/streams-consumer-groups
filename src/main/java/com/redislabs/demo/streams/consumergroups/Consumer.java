package com.redislabs.demo.streams.consumergroups;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;

import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import java.util.List;

public class Consumer extends Agent {

    private static Logger logger = Logger.getLogger(Consumer.class.getName());

    private String name;

    private String group;

    public Consumer(String url, String group) {
        super(url);
        this.name = randomAlphaNumeric(10);
        this.group = group;
        this.init();
    }

    void init() {
        try {
            syncCommands.xgroupCreate(
                    XReadArgs.StreamOffset.latest(STREAM_NAME),
                    group,
                    XGroupCreateArgs.Builder.mkstream()
            );
        } catch (RedisBusyException redisBusyException) {
            // ignore
        }

    }

    void consume() throws InterruptedException {

        logger.info("Consuming messages ...");

        while (true) {
            List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                    io.lettuce.core.Consumer.from(group, name),
                    XReadArgs.StreamOffset.lastConsumed(STREAM_NAME)
            );

            if (!messages.isEmpty()) {
                for (StreamMessage message : messages) {
                    logger.info("Message consumed:");
                    Iterator iterator = message.getBody().entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry) iterator.next();
                        logger.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
                    }

                    // acknowledge message
                    syncCommands.xack(STREAM_NAME, group, message.getId());
                }
            }

            Thread.sleep(getRandomInt(100, 1000));

        }

    }

}
