package com.redislabs.demo.streams.consumergroups;

import java.util.HashMap;
import java.util.Map;

import java.util.logging.Logger;

public class Producer extends Agent {

    private static Logger logger = Logger.getLogger(Producer.class.getName());

    public Producer(String url) {
        super(url);
    }

    void produce() throws InterruptedException {
        logger.info("Producing messages ...");

        while (true) {

            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("speed", String.valueOf(getRandomInt(0, 500)));
            messageBody.put("direction", String.valueOf(getRandomInt(0, 360)));
            messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));

            String messageId = syncCommands.xadd(STREAM_NAME, messageBody);

            logger.info(String.format("Message %s : %s produced", messageId, messageBody));

            Thread.sleep(getRandomInt(100, 1000));

        }

    }
}
