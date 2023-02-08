package com.example;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ProduceConsumeIntegrationTest {
    @Container
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));

    @Test
    void produceConsume_test() {
        String bootstrapServers = kafka.getBootstrapServers();
        String value = "my-value";
        String topic = "my-topic";
        String key = "my-key";
        String group = "testing-consumer-group";

        AtLeastOnceProducer producer = new AtLeastOnceProducer();
        producer.sendMessage(bootstrapServers, topic, key, value);
        String errLogs = kafka.getLogs(OutputFrame.OutputType.STDERR);
        assertTrue(errLogs == null || errLogs.isBlank());

        AtMostOnceConsumer consumer = new AtMostOnceConsumer();
        List<String> messages = consumer.pollMessage(bootstrapServers, topic, group);
        assertEquals(1, messages.size());
        String actual = messages.get(0);
        assertEquals(value, actual);
    }
}
