package com.example.demo.hadoop.model;

import com.example.demo.hadoop.GenericMapper;
import com.example.demo.hadoop.GenericReducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.example.demo.hadoop.model.Aggregator.AVERAGE_SMOKING_PREVALENCE;
import static com.example.demo.hadoop.model.Aggregator.PERCENTAGE_ACCESS_TO_COUNSELING;

public class Test {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Filter filter = new Filter(FilterConfiguration.AGE_GROUP, "\"10-14\"");
        executeJob(new Filter[]{filter}, new Aggregator[]{AVERAGE_SMOKING_PREVALENCE, PERCENTAGE_ACCESS_TO_COUNSELING},
                "hdfs://localhost:9000/input", "hdfs://localhost:9000/output13122");
//        MultipleOutputs.addNamedOutput(job, "filteredData", RawDataOutputFormat.class, NullWritable.class, Text.class);

    }

    public static boolean executeJob(Filter[] filters, Aggregator[] aggregators, String inputDirectory, String outputDirectory) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("filters", objectMapper.writeValueAsString(filters));
        conf.set("aggregators", objectMapper.writeValueAsString(aggregators));

        Job job = Job.getInstance(conf, "test");
        FileInputFormat.addInputPath(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setMapperClass(GenericMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(GenericReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true);
    }
}
