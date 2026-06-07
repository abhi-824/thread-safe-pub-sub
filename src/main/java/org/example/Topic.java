package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

public class Topic {
    private final String id;
    private final String name;
    private final CopyOnWriteArrayList<Subscriber> subscribers;


    public Topic(String name) {
        this.id = UUID.randomUUID().toString();
        this.name = name;
        this.subscribers=new CopyOnWriteArrayList<>();
    }

    public void addSubscriber(Subscriber sub){
        subscribers.add(sub);
    }
    public List<Subscriber> getSubscriberList(){return subscribers;}
}
