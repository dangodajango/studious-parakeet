package com.example.demo.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.example.demo.hadoop.model.Aggregator.AVERAGE_SMOKING_PREVALENCE;
import static com.example.demo.hadoop.model.Aggregator.PERCENTAGE_ACCESS_TO_COUNSELING;

@Slf4j
public class BaseReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    private MultipleOutputs<Text, DoubleWritable> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String[]> rows = new ArrayList<>();
        for (Text value : values) {
            String[] row = value.toString().split(",");
            rows.add(row);
        }
        calculateAverageSmokingPrevalence(key, rows);
        calculatePercentageAccessToCounseling(key, rows);
    }

    private void calculateAverageSmokingPrevalence(Text key, List<String[]> rows) throws IOException, InterruptedException {
        double smokingPrevalence = 0;
        long iterableSize = 0;
        for (String[] row : rows) {
            smokingPrevalence += Double.parseDouble(row[AVERAGE_SMOKING_PREVALENCE.getPosition()].replace("\"", ""));
            iterableSize++;
        }
        assert iterableSize != 0 : "Iterable size was 0";
        double averageSmokingPrevalence = smokingPrevalence / iterableSize;
        multipleOutputs.write("averageSmokingPrevalence", key, new DoubleWritable(averageSmokingPrevalence));
    }

    private void calculatePercentageAccessToCounseling(Text key, List<String[]> rows) throws IOException, InterruptedException {
        long accessToCounseling = 0;
        long iterableSize = 0;
        for (String[] row : rows) {
            boolean hasAccessToCounseling = row[PERCENTAGE_ACCESS_TO_COUNSELING.getPosition()].replace("\"", "").equals("Yes");
            if (hasAccessToCounseling) {
                accessToCounseling++;
            }
            iterableSize++;
        }
        assert iterableSize != 0 : "Iterable size was 0";
        double percentageAccessToCounseling = (accessToCounseling * 100.0) / iterableSize;
        multipleOutputs.write("percentageAccessToCounseling", key, new DoubleWritable(percentageAccessToCounseling));
    }
}
