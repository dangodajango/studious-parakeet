package com.example.demo.hadoop.hdfs.aggregation;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
@Getter
public class AggregationFileContent {

    private final String fileName;
    private final List<AggregationFileRow> rows;
}
