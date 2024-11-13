package com.example.demo.hadoop.filter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class FilterDTO {

    private String filterName;

    private List<String> values;
}
