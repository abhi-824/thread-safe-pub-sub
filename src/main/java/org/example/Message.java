package org.example;

import java.util.UUID;

public class Message {
    private final String id;
    private final long timestamp;
    private final String topic;
    private final Object payload;
    public Message(String topic, Object payload) {
        this.id = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis();
        this.topic = topic;
        this.payload = payload;
    }

    public Object getPayload() {
        return payload;
    }

    String getTopic(){return this.topic;}
}
