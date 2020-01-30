package com.redislabs.demo.streams.consumergroups;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Consumer class.
 */
@SuppressWarnings({"unchecked"})
public class Consumer extends Agent {

    private static Logger LOGGER = LoggerFactory.getLogger(Consumer.class.getName());

    private String name = getRandomAlphaNumeric(10);

    private String group;

    /**
     * @param url A Redis URL.
     * @param group A consumer group name.
     */
    Consumer(String url, String group) {
        super(url);
        this.group = group;
        this.init();
    }

    /**
     * Create the consumer group iff it's not done already.
     */
    void init() {
        LOGGER.info(String.format("Consumer '%s' init ...", name));
        try {
            String ret = syncCommands.xgroupCreate(
                    XReadArgs.StreamOffset.from(STREAM_NAME, "0"),
                    group,
                    XGroupCreateArgs.Builder.mkstream()
            );
            LOGGER.info(String.format("Consumer '%s' created group '%s': %s", name, group, ret));
        } catch (RedisBusyException redisBusyException) {
            LOGGER.info(String.format("Consumer group '%s' already present.", group));
        }
    }

    /**
     * Process a message.
     *
     * @param message The message.
     */
    void process(StreamMessage<String, String> message) {
        LOGGER.info(String.format("Consumer '%s' consumed message with ID '%s'.", name, message.getId()));
    }

    /**
     * Acknowledge a message.
     *
     * @param message The message.
     */
    void ack(StreamMessage<String, String> message) {
        syncCommands.xack(STREAM_NAME, group, message.getId());
    }

    /**
     * Claim all pending messages from other consumers.
     *
     * @param min_idle_time Minimum idle time of messages to claim in ms.
     * @param max_count Maximum amount of messages to claim at once.
     */
    void claim_idle(int min_idle_time, int max_count) {
        List<Object> pending_consumers = syncCommands.xpending(STREAM_NAME, group, Range.unbounded(), Limit.unlimited());
        for (Object pending_consumer : (List<String>) pending_consumers.get(3)) {
            String consumer = String.valueOf(((List<String>) pending_consumer).get(0));
            int count = Math.min(Integer.parseInt(String.valueOf(((List<String>) pending_consumer).get(1))), max_count);

            while (true) {
                if (!consumer.equals(name)) {
                    List<Object> pending_msgs = syncCommands.xpending(STREAM_NAME, io.lettuce.core.Consumer.from(group, consumer), Range.unbounded(), Limit.from(count));
                    if (pending_msgs.isEmpty()) {
                        break;
                    }
                    List<String> pending_msg_ids = new ArrayList<>();
                    for (Object pending_msg : pending_msgs) {
                        pending_msg_ids.add(String.valueOf(((List<String>) pending_msg).get(0)));
                    }
                    List<StreamMessage<String, String>> claims = syncCommands.xclaim(STREAM_NAME, io.lettuce.core.Consumer.from(group, name), min_idle_time, pending_msg_ids.toArray(new String[0]));
                    for (StreamMessage<String, String> claimed : claims) {
                        LOGGER.info(String.format("Consumer '%s' cla1imed message with ID '%s'.", name, claimed.getId()));
                    }
                }
            }
        }
    }

    /**
     * Process pending messages.
     */
    void process_pending() {
        List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                io.lettuce.core.Consumer.from(group, name),
                XReadArgs.StreamOffset.from(STREAM_NAME, "0")
        );
        for (StreamMessage<String, String> message : messages) {
            process(message);
            ack(message);
        }
    }

    @Override
    public void run() {
        LOGGER.info(String.format("Consumer '%s' running ...", name));

        try {
            claim_idle(1000, 10);
            process_pending();

            while (true) {
                List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                        io.lettuce.core.Consumer.from(group, name), XReadArgs.Builder.block(1000),
                        XReadArgs.StreamOffset.lastConsumed(STREAM_NAME)
                );

                for (StreamMessage<String, String> message : messages) {
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
            LOGGER.error(e.getMessage());
        } finally {
            syncCommands.xgroupDelconsumer(STREAM_NAME, io.lettuce.core.Consumer.from(group, name));
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
        LOGGER.info(String.format("Consumer '%s' closed.", name));
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            LOGGER.info("USAGE: Consumer group size");
            System.exit(1);
        }
        String group = args[0];
        int size = Integer.parseInt(args[1]);
        for(int i = 0; i < size; i++) {
            Consumer consumer = new Consumer(REDIS_URL, group);
            consumer.executor.submit(consumer);
        }
    }

}
