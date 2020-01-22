package com.redislabs.demo.streams;

import java.util.HashMap;
import java.util.Map;

public class Producer extends Agent {

    public Producer(String url) {
        super(url);
    }

    void produce(int amount) {
        System.out.println( String.format("\n Sending %s message(s)", amount));

        for (int i = 0 ; i < amount ; i++) {

            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("speed", "15");
            messageBody.put("direction", "270");
            messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));
            messageBody.put("loop_info", String.valueOf( i ));

            String messageId = syncCommands.xadd(STREAMS_NAME, messageBody);

            System.out.println(String.format("\tMessage %s : %s posted", messageId, messageBody));
        }

        System.out.println("\n");

        connection.close();
        redisClient.shutdown();
    }
}
