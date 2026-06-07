# Multithreaded Pub-Sub System

A production-grade publish-subscribe message broker implemented in Java, demonstrating core concurrency patterns including lock-free iteration, async delivery, and backpressure handling.

## Architecture

```mermaid
graph TD
    P1[Publisher 1] --> B[Broker]
    P2[Publisher 2] --> B
    P3[Publisher 3] --> B
    B --> T1[Topic: orders]
    B --> T2[Topic: payments]
    T1 --> S1[Subscriber A\nBounded Queue]
    T1 --> S2[Subscriber B\nBounded Queue]
    T2 --> S3[Subscriber C\nBounded Queue]
    S1 --> CT1[Consumer Thread A]
    S2 --> CT2[Consumer Thread B]
    S3 --> CT3[Consumer Thread C]
```

## Message Lifecycle

```mermaid
sequenceDiagram
    participant P as Publisher
    participant B as Broker
    participant T as Topic
    participant Q as Subscriber Queue
    participant CT as Consumer Thread

    P->>B: publish(topic, payload)
    B->>B: create Message(id, timestamp, payload)
    B->>T: get topic from registry
    T->>T: iterate CopyOnWriteArrayList (no lock)
    T->>Q: enqueue(message) — non-blocking offer()
    Note over Q: if queue full → drop + log
    CT->>Q: take() — blocks until message available
    CT->>CT: messageHandler.accept(message)
```

## Concurrency Challenges & Solutions

### C1: Subscriber List Modification During Iteration

When a publisher iterates the subscriber list to fan out a message, a concurrent unsubscribe can shift list indices — causing active subscribers to be skipped.

```mermaid
sequenceDiagram
    participant PT as Publisher Thread
    participant SL as Subscriber List
    participant UT as Unsubscribe Thread

    PT->>SL: start iteration at index 0
    PT->>SL: deliver to A (index 0)
    PT->>SL: move to index 1
    UT->>SL: remove B → list becomes [A, C]
    PT->>SL: read index 1 → gets C (B was skipped or exception thrown)
    Note over SL: C may be skipped or ConcurrentModificationException thrown
```

**Fix:** `CopyOnWriteArrayList` — mutations copy the entire array atomically. The publisher iterates a stable snapshot; unsubscribes take effect on the next publish, not mid-iteration.

---

### C2: Topic Creation Race (TOCTOU)

Two threads checking if a topic exists and creating it simultaneously can produce two separate Topic objects — splitting subscribers between them.

```mermaid
sequenceDiagram
    participant T1 as Create Thread 1
    participant R as Topic Registry
    participant T2 as Create Thread 2

    T1->>R: check if "orders" exists → does not
    T2->>R: check if "orders" exists → does not
    T1->>R: create Topic("orders")
    T2->>R: create Topic("orders")
    Note over R: two Topic objects created — subscribers split
```

**Fix:** `ConcurrentHashMap.computeIfAbsent()` — collapses check + create into a single atomic operation. Only one Topic is ever created per name.

---

### C3: Slow Subscriber Backpressure

A fast publisher can overwhelm a slow subscriber. Without bounds, the queue grows until OutOfMemoryError.

```mermaid
graph LR
    FP[Fast Publisher\n1000 msg/sec] --> FO[Fanout]
    FO --> QF[Fast Sub Queue\n1000 msg/sec consumed ✅]
    FO --> QS[Slow Sub Queue\n20 msg/sec consumed]
    QS --> OOM[980 msg/sec net growth\n~3.5 GB in 1 hour 💥]
```

**Fix:** Each subscriber gets a `LinkedBlockingQueue` with a bounded capacity. When full, `offer()` returns false and the message is dropped with a log — isolating the slow subscriber without crashing the broker or blocking other subscribers.

---

### C4: Message Ordering Violation

Two publishers publishing simultaneously have no defined order. Even with locking, a thread preempted *before* lock acquisition means the second publisher can enqueue first.

```mermaid
sequenceDiagram
    participant P1 as Publisher 1
    participant P2 as Publisher 2
    participant T as Topic

    P1->>P1: publish(msg1) — preempted by OS before reaching lock
    P2->>T: acquire lock → enqueue msg2 → release
    P1->>T: resumes → acquire lock → enqueue msg1
    Note over T: queue = [msg2, msg1] — order violated
```

**Fix:** Single dedicated publisher thread per topic with an inbox queue. Publishers just drop into the inbox (fast, atomic). The dedicated thread drains it sequentially — whoever enqueued first is processed first.

---

## Synchronization Strategies

```mermaid
graph TD
    subgraph A1[Approach 1: Coarse-Grained Lock]
        GL[Global Lock] --> ALL[Serializes everything]
        ALL --> SLOW[Max ~100 ops/sec\nSlow subscriber blocks entire broker]
    end

    subgraph A2[Approach 2: Fine-Grained Lock]
        TL[Per-Topic Lock] --> PAR[orders and payments\nproceed in parallel]
        PAR --> SLOW2[Slow subscriber\nblocks entire topic]
    end

    subgraph A3[Approach 3: Lock-Free + Async]
        COW[CopyOnWriteArrayList\nlock-free iteration] --> ASYNC[Publisher enqueues\nnever waits]
        ASYNC --> ISO[Full subscriber isolation\nbackpressure via bounded queues]
    end
```

| Strategy | Correctness | Deadlock-free | Concurrency | Slow Subscriber Impact |
|---|---|---|---|---|
| Coarse-grained lock | ✅ | ✅ | Very low | Blocks entire broker |
| Fine-grained lock | ✅ | ✅ | Medium | Blocks entire topic |
| Lock-free + async | ✅ | ✅ | High | Isolated — drops only own messages |

## Class Design

```mermaid
classDiagram
    class Broker {
        -ConcurrentHashMap~String,Topic~ topics
        +registerTopic(name)
        +publish(message)
        +getTopic(name) Topic
    }

    class Topic {
        -String id
        -String name
        -CopyOnWriteArrayList~Subscriber~ subscribers
        +addSubscriber(sub)
        +getSubscriberList() List
    }

    class Message {
        -String id
        -String topic
        -Object payload
        -long timestamp
        +getPayload() Object
        +getTopic() String
    }

    class Publisher {
        -String id
        -Broker broker
        +publish(topic, payload)
    }

    class Subscriber {
        -String id
        -LinkedBlockingQueue~Message~ queue
        -Consumer~Message~ messageHandler
        -Thread consumerThread
        +enqueue(message)
        -consumeLoop()
        +shutdown()
    }

    Broker "1" --> "*" Topic
    Topic "1" --> "*" Subscriber
    Publisher --> Broker
    Broker --> Message
    Message --> Topic
```

## Key Concurrency Primitives

| Primitive | Where Used | Why |
|---|---|---|
| `ConcurrentHashMap` | Broker topic registry | Thread-safe map with atomic `computeIfAbsent` |
| `CopyOnWriteArrayList` | Topic subscriber list | Lock-free reads (publish path); safe snapshot on mutation |
| `LinkedBlockingQueue` | Per-subscriber message queue | Bounded, thread-safe, blocks consumer until message available |
| `volatile` | Subscriber `running` flag | Guarantees immediate cross-thread visibility without locking |
| `offer()` | Subscriber enqueue | Non-blocking — drops message if full instead of blocking publisher |
| `take()` | Consumer loop | Blocks thread cheaply until message available — no busy spin |

## Test Coverage

| Test | What it verifies |
|---|---|
| Basic delivery | Single subscriber receives all messages in order |
| Multiple subscribers | All subscribers on a topic receive every message |
| Slow subscriber | Fast subscriber throughput unaffected by slow one |
| Ordering violation | Two concurrent publishers produce interleaved but internally ordered output |
| Backpressure | Bounded queue drops messages when full; consumer not overwhelmed |

## Running

```bash
mvn compile exec:java -Dexec.mainClass="org.example.Main"
```

Or run `Main.java` directly from IntelliJ.