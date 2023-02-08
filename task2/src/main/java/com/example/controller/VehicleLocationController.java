package com.example.controller;

import com.example.model.VehicleSignal;
import com.example.service.VehicleSignalAcceptor;
import com.example.validate.VehicleSignalValidator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/v1/taxi/location")
@RequiredArgsConstructor
public class VehicleLocationController {

    private final VehicleSignalValidator signalValidator;
    private final VehicleSignalAcceptor vehicleSignalAcceptor;

    @PostMapping("/send")
    public ResponseEntity<String> receiveSignal(@RequestBody VehicleSignal vehicleSignal) {
        if (!signalValidator.validate(vehicleSignal)) {
            return ResponseEntity.unprocessableEntity().body("Signal has incorrect data");
        }
        try {
            vehicleSignalAcceptor.acceptVehicleSignal(vehicleSignal);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Unavailable signal storage");
        }
        return ResponseEntity.accepted().body("Coordinates were accepted");
    }
}
