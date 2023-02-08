package com.example.service;

import com.example.model.VehicleSignal;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class VehicleSignalAcceptor {

    @Value("${spring.kafka.vehicle.signals.vehicle-signals-topic}")
    private String topic;

    private final KafkaTemplate<UUID, VehicleSignal> vehicleSignalKafkaSender;

    public VehicleSignalAcceptor(
            @Qualifier("vehicleSignalKafkaSender")
            KafkaTemplate<UUID, VehicleSignal> vehicleSignalKafkaSender) {

        this.vehicleSignalKafkaSender = vehicleSignalKafkaSender;
    }

    public void acceptVehicleSignal(VehicleSignal signal) throws ExecutionException, InterruptedException, TimeoutException {
        vehicleSignalKafkaSender.send(topic, UUID.randomUUID(), signal).get(2, TimeUnit.MINUTES);
    }

}
