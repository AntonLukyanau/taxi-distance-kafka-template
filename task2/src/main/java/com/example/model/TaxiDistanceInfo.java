package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TaxiDistanceInfo {
    private Long id;
    private LocalDateTime checkIn;
    private double distance;
}
