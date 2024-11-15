package com.example.demo.web;

import com.example.demo.hadoop.hdfs.aggregation.AggregationFileContent;
import com.example.demo.hadoop.hdfs.filter.FilterFileContent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
@Getter
public class Response {

    private final FilterFileContent resultsFromFiltering;

    private final List<AggregationFileContent> resultsFromAggregations;
}
