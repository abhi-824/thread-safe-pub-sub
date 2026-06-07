package org.example;

public class Publisher {
    private final String id;
    private final Broker broker;
    public Publisher(String id, Broker broker) {
        this.id = id;
        this.broker=broker;
    }

    public void publish(String topic,Object payload){
        Message message=new Message(topic,payload);
        broker.publish(message);
    }
}
