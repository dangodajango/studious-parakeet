package com.example.demo.hadoop.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class AggregatedEntry {

    private final String aggregation;

    private final String result;

    public static AggregatedEntry mapToAggregatedEntry(String aggregatedEntryString) {
        String[] entryColumns = aggregatedEntryString.split("\t");
        return new AggregatedEntry(entryColumns[0], entryColumns[1]);
    }
}
