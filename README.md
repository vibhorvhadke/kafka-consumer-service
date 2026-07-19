# Kafka + Spring Boot + MongoDB Microservices — Learning Project

A hands-on learning project demonstrating event-driven microservices using **Apache Kafka**, **Spring Boot**, and **MongoDB**. Consists of two independent Spring Boot services — a **Producer** and a **Consumer** — communicating asynchronously via a Kafka topic.

---

## Tech Stack

- **Java 21**, **Spring Boot 4.0.7**, **Maven**
- **Spring Data MongoDB**, **Spring Kafka**, **Lombok**
- **Apache Kafka** (`apache/kafka:3.8.0` image, KRaft mode — no Zookeeper) via Docker, on `localhost:9092`
- **MongoDB** installed locally (not Docker), on `localhost:27017`
- **IntelliJ IDEA**

---

## Project Structure

### 1. `kafka-producer-service` (port `8080`)

```
com.example.kafkaproducerservice
├── model/User.java              # MongoDB entity (id, name, email)
├── dto/UserEvent.java           # Kafka event DTO (eventType, userId, name, email)
├── repository/UserRepository.java  # MongoRepository<User, String>
├── kafka/UserEventProducer.java    # Publishes UserEvent to "user-events" topic via KafkaTemplate
├── controller/UserController.java  # POST /api/users - saves User to Mongo + publishes Kafka event
└── config/MongoConfig.java         # Custom MongoDatabaseFactory bean (see "Issues & Fixes" below)
```

**Flow:** `POST /api/users` → saves `User` to MongoDB (`vibhordb.users`) → publishes `UserEvent` (keyed by `userId`) to the `user-events` topic.

### 2. `kafka-consumer-service` (port `8081`, scalable to `8082`, `8083`, ...)

```
com.example.kafkaconsumerservice
├── model/User.java              # MongoDB entity (id, name, email)
├── dto/UserEvent.java           # Mirrors producer's DTO (same field names)
├── repository/UserRepository.java  # MongoRepository<User, String>
├── kafka/UserEventConsumer.java     # @KafkaListener - consumes from "user-events", upserts to Mongo
└── config/MongoConfig.java         # Same Mongo fix as producer
```

**Flow:** `@KafkaListener` on `user-events` topic → deserializes `UserEvent` → upserts a `User` document into MongoDB (`vibhordb.users`) using `userId` as the document `_id`.

---

## Kafka Topic

- **Name:** `user-events`
- **Partitions:** `3`
- **Replication factor:** `1`

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic user-events --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

---

## Configuration

### Producer — `application.properties`
```properties
spring.application.name=kafka-producer-service
server.port=8080

spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.producer.properties.spring.json.type.mapping=userEvent:com.example.kafkaproducerservice.dto.UserEvent

spring.data.mongodb.uri=mongodb://localhost:27017/vibhordb
```

### Consumer — `application.properties`
```properties
spring.application.name=kafka-consumer-service
server.port=8081

spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=user-events-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.properties.spring.json.type.mapping=userEvent:com.example.kafkaconsumerservice.dto.UserEvent

spring.data.mongodb.uri=mongodb://localhost:27017/vibhordb
```

To run additional consumer instances (e.g., 8082, 8083), duplicate the IntelliJ run configuration and set a VM option: `-Dserver.port=8082` (etc.), keeping the same `group-id`.

---

## Issues Encountered & Fixes

| Issue | Root Cause | Fix |
|---|---|---|
| Mongo auto-config not reading DB name from URI | Spring Boot 4.0.7 auto-config quirk | Added custom `MongoConfig` bean using `SimpleMongoClientDatabaseFactory` in both services |
| `JsonSerializer`/`JsonDeserializer` broken | Spring Boot 4.0.7 defaults to Jackson 3, incompatible with Spring Kafka's JSON (de)serializers (which expect Jackson 2 / `com.fasterxml.jackson`) | Explicitly added `com.fasterxml.jackson.core:jackson-databind` dependency in both `pom.xml` files |
| `ClassNotFoundException: com.example.kafkaproducerservice.dto.UserEvent` on consumer side | `JsonSerializer` embeds the producer's fully-qualified class name in message headers; consumer has a different package for the same DTO | Added `spring.json.type.mapping=userEvent:<package>.UserEvent` on **both** producer and consumer, using a shared logical alias instead of the real class name |
| All Kafka traffic going to only 1 of 2 consumer instances | Topic accidentally had only **1 partition** (from an unintended `auto.create.topics.enable` recreation after a topic delete) instead of 3 | Used `kafka-topics.sh --alter --topic user-events --partitions 3` to fix partition count without needing to delete the topic |
| Uneven message count across consumers (e.g., 5/5/11 split for 21 messages) | Normal behavior — Kafka hashes the message key (`userId`) to choose a partition; with a small sample size, hash distribution isn't perfectly even | Not a bug — confirmed by checking the "Finished assignment for group" log line, which showed a correct 1:1:1 partition-to-consumer mapping |

---

## Concepts Verified (Consumer Groups & Partitions)

1. **Producer → Kafka → Consumer → MongoDB**, fully verified end-to-end.
2. **Consumer group + partition ownership:** within a consumer group, each partition is owned by exactly one consumer instance at a time — never shared.
3. **Scaling consumers (2 instances, 3 partitions):** partitions split unevenly (e.g., 1 + 2) since 3 doesn't divide evenly by 2; message counts followed partition ownership proportionally.
4. **Scaling consumers to match partition count (3 instances, 3 partitions):** perfect 1:1:1 partition-to-consumer split confirmed via the `Finished assignment for group` log line.
5. **Over-scaling consumers (4 instances, 3 partitions):** the 4th (extra) consumer received `partitions assigned: []` and stayed fully idle — receiving zero messages. Confirms that consumer parallelism is capped by partition count; extra consumers act only as standbys for failover.
6. **Rebalancing:** adding/removing a consumer instance triggers a group rebalance (`partitions revoked` → `partitions assigned`), redistributing partitions among active members.

---

## How to Run

1. Start Kafka (Docker container `kafka`, image `apache/kafka:3.8.0`) and ensure it's listening on `localhost:9092`.
2. Ensure MongoDB is running locally on `localhost:27017`.
3. Create the `user-events` topic with 3 partitions (see command above).
4. Run `kafka-producer-service` (port `8080`).
5. Run one or more instances of `kafka-consumer-service` (ports `8081`, `8082`, ... via `-Dserver.port=<port>` VM option), all sharing `group-id=user-events-group`.
6. Send a test request:
```bash
POST http://localhost:8080/api/users
Content-Type: application/json

{
  "name": "Test User",
  "email": "test@example.com"
}
```
7. Confirm the document is saved in MongoDB (`vibhordb.users`) and the consumer log shows `Received event` / `Saved user to MongoDB`.

---

## Next Steps (Not Yet Implemented)

- Manual partition assignment (consuming from a specific partition instead of relying on group-based auto-assignment)
- Dead-letter topic / error handling for failed message processing
- Additional event types beyond `USER_CREATED`

---

## Author

Vibhor — learning project for Kafka + Spring Boot + Microservices + MongoDB, as part of Senior Java/Solution Architect interview preparation.
