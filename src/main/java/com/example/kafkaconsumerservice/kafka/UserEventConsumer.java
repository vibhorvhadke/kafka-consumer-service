package com.example.kafkaconsumerservice.kafka;

import com.example.kafkaconsumerservice.dto.UserEvent;
import com.example.kafkaconsumerservice.model.User;
import com.example.kafkaconsumerservice.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserEventConsumer {

    private final UserRepository userRepository;

    @KafkaListener(
            topics = "user-events",
            groupId = "user-events-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(UserEvent event) {
        log.info("Received event: {}", event);

        User user = new User(event.getUserId(), event.getName(), event.getEmail());
        userRepository.save(user);

        log.info("Saved user to MongoDB: {}", user);
    }
}