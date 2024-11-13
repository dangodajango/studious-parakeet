package com.example.demo.hadoop.filter;

import java.util.List;

public class PeerInfluenceFilter implements Filter {

    public static final String FILTER_NAME = "peer_influence_filter";

    private static final int POSITION_OF_PEER_INFLUENCE_IN_INPUT_VALUES = 6;

    @Override
    public boolean matchesFilter(String[] value, List<String> requiredPeerInfluences) {
        String peerInfluence = value[POSITION_OF_PEER_INFLUENCE_IN_INPUT_VALUES].replace("\"", "");
        return requiredPeerInfluences.contains(peerInfluence);
    }

    @Override
    public String extractValue(String[] value) {
        return value[POSITION_OF_PEER_INFLUENCE_IN_INPUT_VALUES].replace("\"", "");
    }
}
