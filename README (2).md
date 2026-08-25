# Kafka Producer & Consumer Setup — Change Log

## What was changed

### 1\. `pom.xml` (both `kafka-producer-service` and `kafka-consumer-service`)

Added the correct Spring Kafka dependency:

\<dependency\>

    \<groupId\>org.springframework.kafka\</groupId\>

    \<artifactId\>spring-kafka\</artifactId\>

\</dependency\>

**Why:** The project previously referenced `spring-boot-starter-kafka`, which does not exist as a published artifact. Spring's Kafka integration is maintained as its own project ("Spring for Apache Kafka") under `org.springframework.kafka:spring-kafka`, not under Spring Boot's `spring-boot-starter-*` naming convention. No `<version>` tag is needed — Spring Boot's dependency management (BOM) resolves a compatible version automatically (`4.0.6`, alongside `kafka-clients 4.1.2` transitively).

---

### 2\. `KafkaProducerConfig.java` (new file, producer service)

Added explicit `ProducerFactory<String, UserEvent>` and `KafkaTemplate<String, UserEvent>` beans.

**Why:** Spring Boot auto-configures a generic `KafkaTemplate<Object, Object>` out of the box, but `UserEventProducer` requires a `KafkaTemplate<String, UserEvent>` — a different generic type. Spring's dependency injection matches on generic type parameters, so the auto-configured bean doesn't satisfy the constructor and the app fails to start (`No qualifying bean of type 'KafkaTemplate<String, UserEvent>'`). Defining the bean explicitly with matching generics resolves this.

Key settings included:

- `KEY_SERIALIZER_CLASS_CONFIG` → `StringSerializer` (partition key is `userId`)  
- `VALUE_SERIALIZER_CLASS_CONFIG` → `JacksonJsonSerializer` (Jackson 3–based; replaces the deprecated `JsonSerializer`, which is marked `@Deprecated(forRemoval = true, since = "4.0")` in Spring Kafka 4.x)  
- `ACKS_CONFIG=all`, `ENABLE_IDEMPOTENCE_CONFIG=true`, `RETRIES_CONFIG=3` — production-safe durability settings (no data loss on broker failover, no duplicate sends on retry)

---

### 3\. `KafkaConsumerConfig.java` (new file, consumer service)

Added explicit `ConsumerFactory<String, UserEvent>` and `ConcurrentKafkaListenerContainerFactory<String, UserEvent>` beans.

**Why:** `@KafkaListener` needs a container factory that knows how to deserialize incoming bytes back into a `UserEvent` object. Without it, the listener either fails to start or throws deserialization errors at runtime.

Key settings included:

- `ErrorHandlingDeserializer` wrapping both key and value deserializers — prevents a single malformed ("poison") message from permanently stalling the consumer thread  
- `JacksonJsonDeserializer` as the actual value deserializer, with:  
  - `VALUE_DEFAULT_TYPE` → `UserEvent.class` (tells Jackson what to deserialize into)  
  - `TRUSTED_PACKAGES` → `com.example.kafkaconsumerservice.dto` (whitelist; never use `*` in production — restricts which classes JSON deserialization is allowed to instantiate)  
- `AUTO_OFFSET_RESET_CONFIG=earliest` — new consumer groups start from the beginning of the topic instead of only reading new messages  
- `GROUP_ID_CONFIG=user-events-group` — matches the group id declared in `@KafkaListener`

---

### 4\. `application.properties` (both services)

spring.kafka.bootstrap-servers=localhost:9092

**Why:** Externalizes the broker address instead of hardcoding it inside the config classes, and is injected via `@Value("${spring.kafka.bootstrap-servers}")`.

---

## Files touched

kafka-producer-service/

  pom.xml

  src/main/resources/application.properties

  src/main/java/.../config/KafkaProducerConfig.java   (new)

kafka-consumer-service/

  pom.xml

  src/main/resources/application.properties

  src/main/java/.../config/KafkaConsumerConfig.java   (new)

## Not yet done (flagged for follow-up)

- No `CommonErrorHandler` / `DefaultErrorHandler` attached to the consumer factory yet — deserialization failures from `ErrorHandlingDeserializer` are currently swallowed silently.  
- `spring-kafka-test` (for `@EmbeddedKafka` integration tests) not yet added — add when integration tests are written.

