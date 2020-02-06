package com.redislabs.demo.streams.consumergroups;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer class.
 */
public class Producer extends Agent {

    private static Logger LOGGER = LoggerFactory.getLogger(Producer.class.getName());

    int number;

    /**
     * @param url A Redis URL.
     * @param nr An ordinal number.
     */
    Producer(String url, int nr) {
        super(url);
        this.number = nr;
    }

    @Override
    public void run() {
        LOGGER.info(String.format("Producer %s running ...", number));

        try {
            while (true) {
                Map<String, String> messageBody = new HashMap<>();
                messageBody.put("speed", String.valueOf(getRandomInt(0, 500)));
                messageBody.put("direction", String.valueOf(getRandomInt(0, 360)));
                messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));

                String messageId = syncCommands.xadd(STREAM_NAME, messageBody);

                LOGGER.info(String.format("Producer %d produced message with ID '%s'", number, messageId));

                Thread.sleep(getRandomInt(10, 100));
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
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
        LOGGER.info(String.format("Producer %d closed.", number));
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            LOGGER.info("USAGE: Producer amount");
            System.exit(1);
        }
        int size = Integer.valueOf(args[0]);
        for(int i = 1; i <= size; i++) {
            Producer producer = new Producer(REDIS_URL, i);
            producer.executor.submit(producer);
        }
    }
}
