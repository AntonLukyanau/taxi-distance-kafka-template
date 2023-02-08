package com.example.validate;

import com.example.model.VehicleSignal;
import org.springframework.stereotype.Service;

@Service
public class VehicleSignalValidator {

    public boolean validate(VehicleSignal signal) {
        return idValidate(signal.getId())
                && coordinatesValidate(signal.getLongitude(), signal.getLatitude());
    }

    private boolean idValidate(Long id) {
        return id != null && id > 0;
    }

    private boolean coordinatesValidate(double longitude, double latitude) {
        return longitude > 0 && latitude > 0;
    }

}
