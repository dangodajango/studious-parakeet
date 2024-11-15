package com.example.demo.hadoop.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class AggregatedEntry {

    private final String[] appliedFilters;

    private final String result;

    public static AggregatedEntry mapToAggregatedEntry(String aggregatedEntryString) {
        String[] entryColumns = aggregatedEntryString.split("\t");
        String[] appliedFilters = entryColumns[0].split(",");
        return new AggregatedEntry(appliedFilters, entryColumns[1]);
    }
}
