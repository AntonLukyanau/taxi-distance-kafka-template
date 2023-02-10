package com.example.config;

import com.example.model.TaxiDistanceInfo;
import com.example.model.VehicleSignal;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProcessingConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.vehicle.signals.consumer-group-id}")
    private String consumerGroup;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, VehicleSignal> vehicleSignalKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, VehicleSignal> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(vehicleSignalKafkaListenerConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<Long, VehicleSignal> vehicleSignalKafkaListenerConsumerFactory() {
        Map<String, Object> config = getBaseConfig();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config, new LongDeserializer(), new JsonDeserializer<>(VehicleSignal.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, TaxiDistanceInfo> distanceInfoKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, TaxiDistanceInfo> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(distanceInfoKafkaListenerConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<Long, TaxiDistanceInfo> distanceInfoKafkaListenerConsumerFactory() {
        Map<String, Object> config = getBaseConfig();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config, new LongDeserializer(), new JsonDeserializer<>(TaxiDistanceInfo.class));
    }

    private Map<String, Object> getBaseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return config;
    }

    @Bean
    public ProducerFactory<Long, TaxiDistanceInfo> taxiDistanceInfoProducerFactory() {
        if (bootstrapServers == null) throw new RuntimeException("spring.kafka.bootstrap-servers property wasn't set");
        return new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.RETRIES_CONFIG, 3,
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class
                ));
    }

    @Bean
    public KafkaTemplate<Long, TaxiDistanceInfo> taxiDistanceKafkaSender() {
        return new KafkaTemplate<>(taxiDistanceInfoProducerFactory());
    }
}
