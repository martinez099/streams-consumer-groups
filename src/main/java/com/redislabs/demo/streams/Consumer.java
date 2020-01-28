package com.redislabs.demo.streams;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;

import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import java.util.List;

public class Consumer extends Agent {

    private static Logger logger = Logger.getLogger(Consumer.class.getName());

    private String group;

    public Consumer(String url, String group) {
        super(url);
        this.group = group;
        try {
            syncCommands.xgroupCreate(XReadArgs.StreamOffset.from(STREAMS_NAME, "0-0"), group);
        } catch (RedisBusyException redisBusyException) {
            logger.warning(String.format("Group '%s' already exists", group));
        }
    }

    void consume(String name) {

        logger.info("Waiting for new messages ...");

        while (true) {
            List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                    io.lettuce.core.Consumer.from(group, name),
                    XReadArgs.StreamOffset.lastConsumed(STREAMS_NAME)
            );

            if (!messages.isEmpty()) {
                for (StreamMessage message : messages) {
                    logger.info("Message received:");
                    Iterator<Map.Entry> iterator = message.getBody().entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = iterator.next();
                        logger.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
                    }

                    // acknowledge message
                    syncCommands.xack(STREAMS_NAME, group, message.getId());
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.severe(e.getMessage());
            }
        }

    }

}
