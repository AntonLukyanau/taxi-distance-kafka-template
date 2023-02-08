package com.example.service;

import com.example.model.TaxiDistanceInfo;
import com.example.model.VehicleSignal;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class VehicleSignalsKafkaProcessor {

    @Value("${spring.kafka.vehicle.signals.vehicle-signals-topic}")
    private String vehicleSignalsTopic;
    @Value("${spring.kafka.vehicle.traveled-distance-topic}")
    private String traveledDistanceTopic;

    private final Map<Long, Double> distanceStorage = new ConcurrentHashMap<>();
    private final Map<Long, VehicleSignal> lastReceivedSignals = new ConcurrentHashMap<>();

    @Autowired
    public void processSignalsByDistance(StreamsBuilder builder) {
        KStream<UUID, VehicleSignal> signalsKStream = createVehicleSignalKStream(builder);
        KStream<UUID, TaxiDistanceInfo> distanceInfoKStream = signalsKStream
                .mapValues(this::updateTaxiDistanceInfo);

        JsonDeserializer<TaxiDistanceInfo> distanceInfoJsonDeserializer = new JsonDeserializer<>();
        distanceInfoJsonDeserializer.trustedPackages(TaxiDistanceInfo.class.getPackageName());
        distanceInfoKStream.to(traveledDistanceTopic,
                Produced.with(Serdes.UUID(), Serdes.serdeFrom(new JsonSerializer<>(), distanceInfoJsonDeserializer)));
    }

    private KStream<UUID, VehicleSignal> createVehicleSignalKStream(StreamsBuilder builder) {
        JsonDeserializer<VehicleSignal> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.trustedPackages(VehicleSignal.class.getPackageName());
        return builder.stream(
                vehicleSignalsTopic,
                Consumed.with(Serdes.UUID(), Serdes.serdeFrom(new JsonSerializer<>(), jsonDeserializer))
        );
    }

    private TaxiDistanceInfo updateTaxiDistanceInfo(VehicleSignal vehicleSignal) {
        Long id = vehicleSignal.getId();
        if (lastReceivedSignals.containsKey(id)) {
            return createTaxiDistanceInfo(vehicleSignal);
        }
        return createDefaultInfo(vehicleSignal);
    }

    private TaxiDistanceInfo createDefaultInfo(VehicleSignal vehicleSignal) {
        Long id = vehicleSignal.getId();
        lastReceivedSignals.put(id, vehicleSignal);
        distanceStorage.put(id, 0.0);
        return new TaxiDistanceInfo(id, vehicleSignal.getReceived(), 0);
    }

    private TaxiDistanceInfo createTaxiDistanceInfo(VehicleSignal vehicleSignal) {
        Long id = vehicleSignal.getId();
        VehicleSignal prevSignal = lastReceivedSignals.get(id);
        double distanceSoFar = distanceStorage.get(id);
        double checkInDistance = calculateDistance(prevSignal, vehicleSignal);
        double newDistanceSoFar = distanceSoFar + checkInDistance;
        lastReceivedSignals.put(id, vehicleSignal);
        distanceStorage.put(id, newDistanceSoFar);
        return new TaxiDistanceInfo(id, vehicleSignal.getReceived(), newDistanceSoFar);
    }

    private double calculateDistance(VehicleSignal prev, VehicleSignal curr) {
        double a = prev.getLongitude() - curr.getLongitude();
        double b = prev.getLatitude() - curr.getLatitude();
        return Math.sqrt(a * a + b * b);
    }

}
