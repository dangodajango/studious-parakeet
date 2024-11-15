package com.example.demo.hadoop.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
@Getter
public class SingleAggregationResult {

    private final String aggregationName;

    private final List<AggregatedEntry> aggregatedEntries;
}
