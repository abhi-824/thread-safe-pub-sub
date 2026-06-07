package org.example;

import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        Broker broker = new Broker();
        broker.registerTopic("orders");
        broker.registerTopic("payments");

        System.out.println("=== Test 1: Basic Delivery ===");
        {
            List<String> received = Collections.synchronizedList(new ArrayList<>());
            Subscriber sub = new Subscriber(100, msg -> {
                received.add((String) msg.getPayload());
                System.out.println("  [sub1] received: " + msg.getPayload());
            });
            broker.getTopic("orders").addSubscriber(sub);

            Publisher pub = new Publisher("p1", broker);
            pub.publish("orders", "msg-1");
            pub.publish("orders", "msg-2");
            pub.publish("orders", "msg-3");

            Thread.sleep(500);
            System.out.println("  Result: received " + received.size() + "/3 messages");
//            sub.shutdown();
        }

        System.out.println();
        System.out.println("=== Test 2: Multiple Subscribers ===");
        {
            List<String> receivedA = Collections.synchronizedList(new ArrayList<>());
            List<String> receivedB = Collections.synchronizedList(new ArrayList<>());

            Subscriber subA = new Subscriber(100, msg -> {
                receivedA.add((String) msg.getPayload());
                System.out.println("  [subA] received: " + msg.getPayload());
            });
            Subscriber subB = new Subscriber(100, msg -> {
                receivedB.add((String) msg.getPayload());
                System.out.println("  [subB] received: " + msg.getPayload());
            });

            broker.getTopic("orders").addSubscriber(subA);
            broker.getTopic("orders").addSubscriber(subB);

            Publisher pub = new Publisher("p2", broker);
            pub.publish("orders", "msg-A");
            pub.publish("orders", "msg-B");

            Thread.sleep(500);
            System.out.println("  SubA got: " + receivedA);
            System.out.println("  SubB got: " + receivedB);
//            subA.shutdown();
//            subB.shutdown();
        }

        // clear subscribers before next test
        broker.registerTopic("orders");

        System.out.println();
        System.out.println("=== Test 3: Slow Subscriber Doesnt Block Fast ===");
        {
            broker.registerTopic("speed");
            List<Long> fastTimings = Collections.synchronizedList(new ArrayList<>());

            Subscriber fastSub = new Subscriber(100, msg -> {
                fastTimings.add(System.currentTimeMillis());
                System.out.println("  [fast] received: " + msg.getPayload() + " at t+" + (System.currentTimeMillis()));
            });
            Subscriber slowSub = new Subscriber(100, msg -> {
                try { Thread.sleep(800); } catch (InterruptedException e) {}
                System.out.println("  [slow] received: " + msg.getPayload());
            });

            broker.getTopic("speed").addSubscriber(fastSub);
            broker.getTopic("speed").addSubscriber(slowSub);

            Publisher pub = new Publisher("p3", broker);
            long start = System.currentTimeMillis();
            pub.publish("speed", "msg-1");
            pub.publish("speed", "msg-2");
            pub.publish("speed", "msg-3");

            Thread.sleep(500);
            long fastDuration = fastTimings.isEmpty() ? -1 : fastTimings.get(fastTimings.size()-1) - start;
            System.out.println("  Fast sub got all 3 msgs in ~" + fastDuration + "ms (should be << 800ms)");
//            fastSub.shutdown();
//            slowSub.shutdown();
        }

        System.out.println();
        System.out.println("=== Test 4: Ordering Violation (run multiple times to see) ===");
        {
            broker.registerTopic("ordering");
            List<String> received = Collections.synchronizedList(new ArrayList<>());

            Subscriber sub = new Subscriber(100, msg -> {
                received.add((String) msg.getPayload());
            });
            broker.getTopic("ordering").addSubscriber(sub);

            Publisher p1 = new Publisher("p1", broker);
            Publisher p2 = new Publisher("p2", broker);

            Thread t1 = new Thread(() -> {
                p1.publish("ordering", "P1-1");
                p1.publish("ordering", "P1-2");
                p1.publish("ordering", "P1-3");
            });
            Thread t2 = new Thread(() -> {
                p2.publish("ordering", "P2-1");
                p2.publish("ordering", "P2-2");
                p2.publish("ordering", "P2-3");
            });

            t1.start(); t2.start();
            t1.join();  t2.join();
            Thread.sleep(500);

            System.out.println("  Received order: " + received);
            System.out.println("  P1 in order: " + isInOrder(received, "P1-1", "P1-2", "P1-3"));
            System.out.println("  P2 in order: " + isInOrder(received, "P2-1", "P2-2", "P2-3"));
            System.out.println("  Interleaved: " + isInterleaved(received));
//            sub.shutdown();
        }

        System.out.println();
        System.out.println("=== Test 5: Backpressure / Queue Full ===");
        {
            broker.registerTopic("backpressure");
            List<String> consumed = Collections.synchronizedList(new ArrayList<>());

            Subscriber sub = new Subscriber(3, msg -> {
                try { Thread.sleep(300); } catch (InterruptedException e) {}
                consumed.add((String) msg.getPayload());
                System.out.println("  [sub] consumed: " + msg.getPayload());
            });
            broker.getTopic("backpressure").addSubscriber(sub);

            Publisher pub = new Publisher("p5", broker);
            for (int i = 1; i <= 10; i++) {
                pub.publish("backpressure", "msg-" + i);
            }

            Thread.sleep(3000);
            System.out.println("  Consumed " + consumed.size() + "/10 (rest dropped due to full queue)");
//            sub.shutdown();
        }

        System.out.println();
        System.out.println("Done.");
    }

    static boolean isInOrder(List<String> list, String a, String b, String c) {
        int ia = list.indexOf(a), ib = list.indexOf(b), ic = list.indexOf(c);
        return ia != -1 && ib != -1 && ic != -1 && ia < ib && ib < ic;
    }

    static boolean isInterleaved(List<String> list) {
        for (int i = 0; i < list.size() - 1; i++) {
            String curr = list.get(i), next = list.get(i + 1);
            if (curr.startsWith("P1") && next.startsWith("P2")) return true;
            if (curr.startsWith("P2") && next.startsWith("P1")) return true;
        }
        return false;
    }
}