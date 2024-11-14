package com.example.demo.web;

import com.example.demo.hadoop.model.AggregatedEntry;
import com.example.demo.hadoop.model.FilteredEntry;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
@Getter
public class Response {

    private final List<FilteredEntry> resultsFromFiltering;

    private final List<List<AggregatedEntry>> resultsFromAggregations;
}
