package org.example;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {
    private final ConcurrentHashMap<String,Topic> topicConcurrentHashMap;

    public Broker() {
        this.topicConcurrentHashMap = new ConcurrentHashMap<>();
    }

    public void registerTopic(String topicName){
        topicConcurrentHashMap.computeIfAbsent(topicName,Topic::new);
    }
    public Topic getTopic(String topic){
        return topicConcurrentHashMap.get(topic);
    }
    public void publish(Message message){
        Topic topic=topicConcurrentHashMap.get(message.getTopic());
        if(topic==null){
            return;
        }
        List<Subscriber> subscriberList= topic.getSubscriberList();
        for(Subscriber sub: subscriberList){
            sub.enqueue(message);
        }
    }
}
