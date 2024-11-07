package com.example.demo.hadoop.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter
@NoArgsConstructor
@AllArgsConstructor
public class Filter {

    private FilterConfiguration filterConfiguration;

    private String value;
}
