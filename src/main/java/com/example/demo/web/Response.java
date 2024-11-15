package com.example.demo.web;

import com.example.demo.hadoop.model.FilteredEntry;
import com.example.demo.hadoop.model.SingleAggregationResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
@Getter
public class Response {

    private final List<FilteredEntry> resultsFromFiltering;

    private final List<SingleAggregationResult> resultsFromAggregations;
}
