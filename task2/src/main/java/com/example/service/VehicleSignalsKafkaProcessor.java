package com.example.service;

import com.example.model.TaxiDistanceInfo;
import com.example.model.VehicleSignal;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class VehicleSignalsKafkaProcessor {

    @Value("${spring.kafka.vehicle.traveled-distance-topic}")
    private String traveledDistanceTopic;
    private final KafkaTemplate<Long, TaxiDistanceInfo> distanceInfoSender;

    private final Map<Long, Double> distanceStorage = new ConcurrentHashMap<>();
    private final Map<Long, VehicleSignal> lastReceivedSignals = new ConcurrentHashMap<>();

    public VehicleSignalsKafkaProcessor(
            @Qualifier("taxiDistanceKafkaSender")
            KafkaTemplate<Long, TaxiDistanceInfo> distanceInfoSender) {
        this.distanceInfoSender = distanceInfoSender;
    }

    @KafkaListener(
            topics = "${spring.kafka.vehicle.signals.vehicle-signals-topic}",
            concurrency = "3",
            groupId = "${spring.kafka.vehicle.signals.consumer-group-id}",
            containerFactory = "vehicleSignalKafkaListenerFactory")
    public void consumeJson(VehicleSignal vehicleSignal) {
        TaxiDistanceInfo taxiDistanceInfo = updateTaxiDistanceInfo(vehicleSignal);
        System.out.println(Thread.currentThread().getName() + " принял");
        distanceInfoSender.send(traveledDistanceTopic, taxiDistanceInfo.getId(), taxiDistanceInfo);
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
