package com.example.demo.hadoop.filter;

import java.util.List;

public class AgeGroupFilter implements Filter {

    public static final String FILTER_NAME = "age_group_filter";

    private static final int POSITION_OF_AGE_GROUP_LOWER_BOUND = 0;

    private static final int POSITION_OF_AGE_GROUP_UPPER_BOUND = 1;

    private static final int POSITION_OF_AGE_GROUP_IN_INPUT_VALUES = 1;

    @Override
    public boolean matchesFilter(String[] value, List<String> filterValues) {
        int ageGroupLowerBound = Integer.parseInt(filterValues.get(POSITION_OF_AGE_GROUP_LOWER_BOUND));
        int ageGroupUpperBound = Integer.parseInt(filterValues.get(POSITION_OF_AGE_GROUP_UPPER_BOUND));

        String ageGroupAsString = value[POSITION_OF_AGE_GROUP_IN_INPUT_VALUES].replace("\"", "");
        if (ageGroupAsString.equals("80+")) {
            return ageGroupUpperBound >= 80;
        } else {
            String[] ageGroup = ageGroupAsString.split("-");
            return ageGroupLowerBound <= Integer.parseInt(ageGroup[POSITION_OF_AGE_GROUP_LOWER_BOUND])
                   && ageGroupUpperBound >= Integer.parseInt(ageGroup[POSITION_OF_AGE_GROUP_UPPER_BOUND]);
        }
    }

    @Override
    public String extractValue(String[] value) {
        return value[POSITION_OF_AGE_GROUP_IN_INPUT_VALUES].replace("\"", "");
    }
}
