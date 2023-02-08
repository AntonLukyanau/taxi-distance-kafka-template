package com.example.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class KafkaConfiguration {

    private Environment environment;

    @Autowired
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        String bootstrapServers = environment.getProperty("spring.kafka.properties.bootstrap.servers");
        if (bootstrapServers == null) throw new RuntimeException("spring.kafka.properties.bootstrap.servers property wasn't set");
        return new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.RETRIES_CONFIG, 0,
                        ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                ));
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        String bootstrapServers = environment.getProperty("spring.kafka.properties.bootstrap.servers");
        if (bootstrapServers == null) throw new RuntimeException("spring.kafka.properties.bootstrap.servers property wasn't set");
        return new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));
    }

    @Bean("inputTopic")
    public NewTopic inputTopic() {
        return TopicBuilder.name("input").partitions(3).replicas(2).build();
    }

    @Bean("outputTopic")
    public NewTopic outputTopic() {
        return TopicBuilder.name("output").partitions(3).replicas(2).build();
    }

}
