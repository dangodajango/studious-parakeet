package com.example.demo.hadoop;

import com.example.demo.hadoop.model.Aggregator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.example.demo.hadoop.model.Aggregator.AVERAGE_SMOKING_PREVALENCE;
import static com.example.demo.hadoop.model.Aggregator.PERCENTAGE_ACCESS_TO_COUNSELING;

@Slf4j
public class AggregationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        final Aggregator[] aggregators = objectMapper.readValue(context.getConfiguration().get("aggregators"), Aggregator[].class);

        final List<String[]> rows = new ArrayList<>();
        for (final Text value : values) {
            final String[] row = value.toString().split(",");
            rows.add(row);
        }

        for (final Aggregator aggregator : aggregators) {
            if (aggregator.equals(AVERAGE_SMOKING_PREVALENCE)) {
                calculateAverageSmokingPrevalence(rows, context);
            } else {
                calculatePercentageAccessToCounseling(rows, context);
            }
        }
    }

    private void calculateAverageSmokingPrevalence(final List<String[]> rows, final Context context) throws IOException, InterruptedException {
        double smokingPrevalence = 0;
        long iterableSize = 0;
        for (final String[] row : rows) {
            smokingPrevalence += Double.parseDouble(row[AVERAGE_SMOKING_PREVALENCE.getPosition()].replace("\"", ""));
            iterableSize++;
        }
        assert iterableSize != 0 : "Iterable size was 0";
        final double averageSmokingPrevalence = smokingPrevalence / iterableSize;
        context.write(new Text("average-smoking-prevalence"), new DoubleWritable(averageSmokingPrevalence));
    }

    private void calculatePercentageAccessToCounseling(final List<String[]> rows, final Context context) throws IOException, InterruptedException {
        long accessToCounseling = 0;
        long iterableSize = 0;
        for (final String[] row : rows) {
            final boolean hasAccessToCounseling = row[PERCENTAGE_ACCESS_TO_COUNSELING.getPosition()].replace("\"", "").equals("Yes");
            if (hasAccessToCounseling) {
                accessToCounseling++;
            }
            iterableSize++;
        }
        assert iterableSize != 0 : "Iterable size was 0";
        final double percentageAccessToCounseling = (accessToCounseling * 100.0) / iterableSize;
        context.write(new Text("percentage-access-to-counseling"), new DoubleWritable(percentageAccessToCounseling));
    }
}
