package com.example.demo.hadoop.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum Aggregator {
    AVERAGE_SMOKING_PREVALENCE(3),
    PERCENTAGE_ACCESS_TO_COUNSELING(10);

    /**
     * The position of the aggregated field in the received csv file.
     */
    private final int position;
}
