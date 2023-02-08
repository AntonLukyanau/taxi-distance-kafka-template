package com.example.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaTopicService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private Environment environment;

    @Autowired
    public KafkaTopicService(KafkaTemplate<String, String> kafkaTemplate, Environment environment) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Autowired
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public void sendMessage(String message) {
        String topic = environment.getProperty("spring.kafka.listener.default.topic");
        if (topic == null) throw new RuntimeException("spring.kafka.listener.default.topic property wasn't set");
        kafkaTemplate.send(topic, message);
        kafkaTemplate.flush();
    }

}
