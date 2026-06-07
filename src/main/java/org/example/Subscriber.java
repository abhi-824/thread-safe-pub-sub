package org.example;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class Subscriber {
    private final String id;
    private final BlockingQueue<Message> messageBlockingQueue;
    private final Thread consumerThread;
    private final Consumer<Message> messageHandler;

    public Subscriber(int queueCapacity, Consumer<Message> messageHandler) {
        this.id = UUID.randomUUID().toString();
        this.messageBlockingQueue = new LinkedBlockingQueue<>(queueCapacity);;
        this.consumerThread = new Thread(this::consumeLoop, "subscriber-" + id);
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
        this.messageHandler=messageHandler;
    }

    void enqueue(Message message){
        boolean accepted=messageBlockingQueue.offer(message);
        if(!accepted){
            System.out.println("Queue full");
        }
    }

    void consumeLoop(){
        while(true){
            try{
                Message message=messageBlockingQueue.take();
                messageHandler.accept(message);
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                break;
            }

        }
    }

}
