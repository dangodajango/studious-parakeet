package com.example.demo.hadoop.hdfs.aggregation;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class AggregationFileRow {
    private final String[] columns;
    private final String value;
}
