package com.redislabs.demo.streams.consumergroups;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.util.logging.Logger;

public class Producer extends Agent {

    private static Logger logger = Logger.getLogger(Producer.class.getName());

    Producer(String url) {
        super(url);
    }

    @Override
    public void run() {
        logger.info("Producer running ...");


        try {
            while (true) {
                Map<String, String> messageBody = new HashMap<>();
                messageBody.put("speed", String.valueOf(getRandomInt(0, 500)));
                messageBody.put("direction", String.valueOf(getRandomInt(0, 360)));
                messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));

                String messageId = syncCommands.xadd(STREAM_NAME, messageBody);

                logger.info(String.format("Message with ID '%s' produced.", messageId));

                Thread.sleep(getRandomInt(10, 100));

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
        logger.info("Producer closed.");
    }
}
