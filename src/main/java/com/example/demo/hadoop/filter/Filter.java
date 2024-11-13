package com.example.demo.hadoop.filter;

import java.util.List;

public interface Filter {

    boolean matchesFilter(String[] value, List<String> filterValues);

    String extractValue(String[] value);
}
