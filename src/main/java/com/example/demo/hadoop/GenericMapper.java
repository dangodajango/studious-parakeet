package com.example.demo.hadoop;

import com.example.demo.hadoop.model.Filter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class GenericMapper extends Mapper<Object, Text, Text, Text> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(final Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    /**
     * @param value - Csv line, in this format: "Year","Age_Group","Gender","Smoking_Prevalence","Drug_Experimentation","Socioeconomic_Status","Peer_Influence","School_Programs","Family_Background","Mental_Health","Access_to_Counseling","Parental_Supervision","Substance_Education","Community_Support","Media_Influence"
     */
    @Override
    protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] data = value.toString().split(",");
        final Filter[] filters = objectMapper.readValue(context.getConfiguration().get("filters"), Filter[].class);
        for (final Filter filter : filters) {
            final int positionOfFilteredField = filter.getFilterConfiguration().getPosition();
            if (!data[positionOfFilteredField].equals(filter.getValue())) {
                return;
            }
        }
        if (context.getConfiguration().get("aggregators") != null) {
            context.write(new Text(context.getJobName()), value);
        }
    }
}
