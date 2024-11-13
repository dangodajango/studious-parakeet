package com.example.demo.hadoop.filter;

import java.util.List;

public class GenderFilter implements Filter {

    public static final String FILTER_NAME = "gender_filter";

    private static final int POSITION_OF_GENDER_IN_INPUT_VALUES = 2;

    @Override
    public boolean matchesFilter(String[] value, List<String> requiredGenders) {
        String gender = value[POSITION_OF_GENDER_IN_INPUT_VALUES].replace("\"", "");
        return requiredGenders.contains(gender);
    }

    @Override
    public String extractValue(String[] value) {
        return value[POSITION_OF_GENDER_IN_INPUT_VALUES].replace("\"", "");
    }
}
