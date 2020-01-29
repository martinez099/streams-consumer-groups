package com.redislabs.demo.streams.consumergroups;

import io.lettuce.core.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import java.util.List;

public class Consumer extends Agent {

    private static Logger logger = Logger.getLogger(Consumer.class.getName());

    private String name = randomAlphaNumeric(10);

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
            // ignore
        }
    }

    void process(StreamMessage message) {
        logger.info(String.format("Message with ID '%s' consumed by %s.", message.getId(), name));
    }

    void ack(StreamMessage message) {
        syncCommands.xack(STREAM_NAME, group, message.getId());
    }

    void claim_idle(int min_idle_time, int max_count) {
        List idles = syncCommands.xpending(STREAM_NAME, group, Range.unbounded(), Limit.unlimited());
        for (Object idle : (List) idles.get(3)) {
            List idle_list = (List) idle;
            String from = String.valueOf(idle_list.get(0));
            int count = Math.min(Integer.valueOf(String.valueOf(idle_list.get(1))), max_count);
            while (true) {
                if (!from.equals(name)) {
                    List idle_msgs = syncCommands.xpending(STREAM_NAME, io.lettuce.core.Consumer.from(group, from), Range.unbounded(), Limit.from(count));
                    if (idle_msgs.isEmpty()) {
                        break;
                    }
                    List<String> idle_msg_ids = new ArrayList<>();
                    for (Object idle_msg : idle_msgs) {
                        idle_msg_ids.add(String.valueOf(((List) idle_msg).get(0)));
                    }
                    List<StreamMessage<String, String>> claims = syncCommands.xclaim(STREAM_NAME, io.lettuce.core.Consumer.from(group, name), min_idle_time, idle_msg_ids.toArray(new String[0]));
                    for (StreamMessage claimed : claims) {
                        logger.info(String.format("Consumer '%s' claimed message with ID '%s'.", name, claimed.getId()));
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
            List<StreamMessage<String, String>> messages;
            while (true) {
                messages = syncCommands.xreadgroup(
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


}
