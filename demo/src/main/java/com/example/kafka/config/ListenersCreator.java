package com.example.kafka.config;

import com.example.kafka.service.KafkaConsumerListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ListenersCreator {

    @Bean("topicListenerOne")
    public KafkaConsumerListener topicListenerOne() {
        return new KafkaConsumerListener(1);
    }

    @Bean("topicListenerTwo")
    public KafkaConsumerListener topicListenerTwo() {
        return new KafkaConsumerListener(2);
    }

    @Bean("topicListenerThree")
    public KafkaConsumerListener topicListenerThree() {
        return new KafkaConsumerListener(3);
    }

}
