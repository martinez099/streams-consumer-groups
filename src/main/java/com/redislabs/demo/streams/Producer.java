package com.redislabs.demo.streams;

import java.util.HashMap;
import java.util.Map;

import java.util.logging.Logger;

public class Producer extends Agent {

    private static Logger logger = Logger.getLogger(Producer.class.getName());

    public Producer(String url) {
        super(url);
    }

    void produce(int amount) {
        logger.info( String.format("Sending %s message(s)", amount));

        for (int i = 0 ; i < amount ; i++) {

            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("speed", "15");
            messageBody.put("direction", "270");
            messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));
            messageBody.put("loop_info", String.valueOf( i ));

            String messageId = syncCommands.xadd(STREAMS_NAME, messageBody);

            logger.info(String.format("Message %s : %s posted", messageId, messageBody));
        }

    }
}
