package com.example.service;

import com.example.model.VehicleSignal;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.context.annotation.RequestScope;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Scope("prototype")
public class VehicleSignalAcceptor {

    @Value("${spring.kafka.vehicle.signals.vehicle-signals-topic}")
    private String topic;

    private final KafkaTemplate<Long, VehicleSignal> vehicleSignalKafkaSender;

    public VehicleSignalAcceptor(
            @Qualifier("vehicleSignalKafkaSender")
            KafkaTemplate<Long, VehicleSignal> vehicleSignalKafkaSender) {

        this.vehicleSignalKafkaSender = vehicleSignalKafkaSender;
    }

    public void acceptVehicleSignal(VehicleSignal signal) throws ExecutionException, InterruptedException, TimeoutException {
        vehicleSignalKafkaSender.send(topic, signal.getId(), signal).get(2, TimeUnit.MINUTES);
    }

}
