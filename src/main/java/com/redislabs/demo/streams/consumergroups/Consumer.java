package com.redislabs.demo.streams.consumergroups;

import io.lettuce.core.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import java.util.List;

public class Consumer extends Agent {

    private static Logger logger = Logger.getLogger(Consumer.class.getName());

    private String name = getRandomAlphaNumeric(10);

    private String group;

    Consumer(String url, String group) {
        super(url);
        this.group = group;
        this.init();
    }

    void init() {
        logger.info(String.format("Consumer '%s' init ...", name));
        try {
            String ret = syncCommands.xgroupCreate(
                    XReadArgs.StreamOffset.from(STREAM_NAME, "0"),
                    group,
                    XGroupCreateArgs.Builder.mkstream()
            );
            logger.info(String.format("Consumer '%s' created group '%s': %s", name, group, ret));
        } catch (RedisBusyException redisBusyException) {
            logger.info(String.format("Consumer group '%s' already present.", group));
        }
    }

    void process(StreamMessage message) {
        logger.info(String.format("Consumer '%s' consumed message with ID '%s'.", name, message.getId()));
    }

    void ack(StreamMessage message) {
        syncCommands.xack(STREAM_NAME, group, message.getId());
    }

    void claim_idle(int min_idle_time, int max_count) {
        List pending_consumers = syncCommands.xpending(STREAM_NAME, group, Range.unbounded(), Limit.unlimited());
        for (Object pending_consumer : (List) pending_consumers.get(3)) {
            String consumer = String.valueOf(((List) pending_consumer).get(0));
            int count = Math.min(Integer.valueOf(String.valueOf(((List) pending_consumer).get(1))), max_count);

            while (true) {
                if (!consumer.equals(name)) {
                    List pending_msgs = syncCommands.xpending(STREAM_NAME, io.lettuce.core.Consumer.from(group, consumer), Range.unbounded(), Limit.from(count));
                    if (pending_msgs.isEmpty()) {
                        break;
                    }
                    List<String> pending_msg_ids = new ArrayList<>();
                    for (Object pending_msg : pending_msgs) {
                        pending_msg_ids.add(String.valueOf(((List) pending_msg).get(0)));
                    }
                    List<StreamMessage<String, String>> claims = syncCommands.xclaim(STREAM_NAME, io.lettuce.core.Consumer.from(group, name), min_idle_time, pending_msg_ids.toArray(new String[0]));
                    for (StreamMessage claimed : claims) {
                        logger.info(String.format("Consumer '%s' cla1imed message with ID '%s'.", name, claimed.getId()));
                    }
                }
            }
        }
    }

    void process_pending() {
        List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                io.lettuce.core.Consumer.from(group, name),
                XReadArgs.StreamOffset.from(STREAM_NAME, "0")
        );
        for (StreamMessage message : messages) {
            process(message);
            ack(message);
        }
    }

    @Override
    public void run() {
        logger.info(String.format("Consumer '%s' running ...", name));

        try {
            claim_idle(1000, 10);
            process_pending();

            while (true) {
                List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                        io.lettuce.core.Consumer.from(group, name), XReadArgs.Builder.block(1000),
                        XReadArgs.StreamOffset.lastConsumed(STREAM_NAME)
                );

                for (StreamMessage message : messages) {
                    process(message);

                    if (RANDOM.nextInt() < (Integer.MAX_VALUE - 100000000)) {
                        ack(message);
                    } else {
                        throw new RuntimeException("I'm broke!");
                    }
                }

                Thread.sleep(getRandomInt(100, 1000));
            }
        } catch (Exception e) {
            logger.severe(e.getMessage());
        } finally {
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        logger.info(String.format("Consumer '%s' closed.", name));
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            logger.info("USAGE: Consumer group size");
            System.exit(1);
        }
        String group = args[0];
        int size = Integer.valueOf(args[1]);
        for(int i = 0; i < size; i++) {
            Consumer consumer = new Consumer(REDIS_URL, group);
            consumer.executor.submit(consumer);
        }
    }

}
