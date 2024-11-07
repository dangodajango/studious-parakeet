package com.example.demo.hadoop.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum FilterConfiguration {

    AGE_GROUP(1),
    GENDER(2),
    PEER_INFLUENCE(6);

    /**
     * The position of the filtered field in the received csv file.
     */
    private final int position;


}
