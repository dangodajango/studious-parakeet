package com.example.demo.hadoop;

import com.example.demo.hadoop.filter.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class BaseMapper extends Mapper<Object, Text, Text, Text> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, Filter> filters = new HashMap<>();

    private FilterDTO[] filterDTOs;

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws JsonProcessingException {
        multipleOutputs = new MultipleOutputs<>(context);
        filters.put(AgeGroupFilter.FILTER_NAME, new AgeGroupFilter());
        filters.put(GenderFilter.FILTER_NAME, new GenderFilter());
        filters.put(PeerInfluenceFilter.FILTER_NAME, new PeerInfluenceFilter());
        filterDTOs = objectMapper.readValue(context.getConfiguration().get("filters"), FilterDTO[].class);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    /**
     * @param value - Csv line, in this format: "Year","Age_Group","Gender","Smoking_Prevalence","Drug_Experimentation","Socioeconomic_Status","Peer_Influence","School_Programs","Family_Background","Mental_Health","Access_to_Counseling","Parental_Supervision","Substance_Education","Community_Support","Media_Influence"
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] deconstructedValue = value.toString().split(",");
        StringBuilder reducerKey = new StringBuilder();

        for (FilterDTO filterDTO : filterDTOs) {
            Filter filter = filters.get(filterDTO.getFilterName());
            boolean isMatching = filter.matchesFilter(deconstructedValue, filterDTO.getValues());
            if (!isMatching) {
                return;
            }
            reducerKey.append(filter.extractValue(deconstructedValue)).append(" | ");
        }
        context.write(new Text(reducerKey.toString()), value);
        multipleOutputs.write(context.getJobName(), null, value);
    }
}
