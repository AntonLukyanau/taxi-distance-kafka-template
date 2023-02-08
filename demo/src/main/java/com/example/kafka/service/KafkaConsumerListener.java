package com.example.kafka.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

@RequiredArgsConstructor
public class KafkaConsumerListener {

    private final int id;

    @KafkaListener(
            topics = "${spring.kafka.listener.default.topic}",
            groupId = "${spring.kafka.listener.default.consumer-group-id}")
    void consume(ConsumerRecord<String, String> message) {
        System.out.println("Listener " + id + " has received a message: " + message.value() + " with key: " + message.key());
    }

}
