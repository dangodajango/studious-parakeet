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
public class GenericReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Aggregator[] aggregators = objectMapper.readValue(context.getConfiguration().get("aggregators"), Aggregator[].class);

        final List<String[]> rows = new ArrayList<>();
        for (Text value : values) {
            String[] row = value.toString().split(",");
            rows.add(row);
        }

        for (Aggregator aggregator : aggregators) {
            if (aggregator.equals(AVERAGE_SMOKING_PREVALENCE)) {
                calculateAverageSmokingPrevalence(key, rows, context);
            } else {
                calculatePercentageAccessToCounseling(key, rows, context);
            }
        }
    }

    private void calculateAverageSmokingPrevalence(final Text key, final List<String[]> rows, final Context context) throws IOException, InterruptedException {
        double smokingPrevalence = 0;
        long iterableSize = 0;
        for (String[] row : rows) {
            smokingPrevalence += Double.parseDouble(row[AVERAGE_SMOKING_PREVALENCE.getPosition()].replace("\"", ""));
            iterableSize++;
        }
        assert iterableSize != 0 : "Iterable size was 0";
        final double averageSmokingPrevalence = smokingPrevalence / iterableSize;
        context.write(new Text(key.toString() + "-average-smoking-prevalence"), new DoubleWritable(averageSmokingPrevalence));
    }

    private void calculatePercentageAccessToCounseling(final Text key, final List<String[]> rows, final Context context) throws IOException, InterruptedException {
        long accessToCounseling = 0;
        long iterableSize = 0;
        for (String[] row : rows) {
            final boolean hasAccessToCounseling = row[PERCENTAGE_ACCESS_TO_COUNSELING.getPosition()].replace("\"", "").equals("Yes");
            if (hasAccessToCounseling) {
                accessToCounseling++;
            }
            iterableSize++;
        }
        assert iterableSize != 0 : "Iterable size was 0";
        final double percentageAccessToCounseling = (accessToCounseling * 100.0) / iterableSize;
        context.write(new Text(key.toString() + "-percentage-access-to-counseling"), new DoubleWritable(percentageAccessToCounseling));
    }
}
