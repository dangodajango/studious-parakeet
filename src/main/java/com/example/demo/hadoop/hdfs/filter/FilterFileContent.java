package com.example.demo.hadoop.hdfs.filter;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@Getter
@RequiredArgsConstructor
public class FilterFileContent {
    private final List<FilterFileRow> rows;
}
