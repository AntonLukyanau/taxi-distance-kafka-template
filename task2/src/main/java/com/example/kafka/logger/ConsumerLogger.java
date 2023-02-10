package com.example.kafka.logger;

import com.example.model.TaxiDistanceInfo;
import com.example.model.VehicleSignal;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;

@Service
@Log4j2
public class ConsumerLogger {
    @KafkaListener(
            topics = "${spring.kafka.vehicle.traveled-distance-topic}",
            concurrency = "1",
            groupId = "${spring.kafka.application-name}",
            containerFactory = "vehicleSignalKafkaListenerFactory")
    public void consumeJson(TaxiDistanceInfo distanceInfo) {
        log.info(String.format("""
                Taxi #%d traveled %.2f at %s
                """,
                distanceInfo.getId(),
                distanceInfo.getDistance(),
                distanceInfo.getCheckIn().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
    }
}
