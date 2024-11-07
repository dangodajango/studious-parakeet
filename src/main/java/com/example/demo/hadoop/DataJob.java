package com.example.demo.hadoop;

import com.example.demo.hadoop.model.Aggregator;
import com.example.demo.hadoop.model.Filter;
import com.example.demo.hadoop.model.FilterConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.UUID;

import static com.example.demo.hadoop.model.Aggregator.AVERAGE_SMOKING_PREVALENCE;
import static com.example.demo.hadoop.model.Aggregator.PERCENTAGE_ACCESS_TO_COUNSELING;

public class DataJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Filter filter = new Filter(FilterConfiguration.AGE_GROUP, "\"10-14\"");
        Filter filter2 = new Filter(FilterConfiguration.GENDER, "\"Both\"");
        executeJob(new Filter[]{filter}, new Aggregator[]{AVERAGE_SMOKING_PREVALENCE, PERCENTAGE_ACCESS_TO_COUNSELING},
                new Path("/input"), new Path("/output"));
    }

    public static String executeJob(final Filter[] filters, final Aggregator[] aggregators, final Path inputDirectory, final Path outputDirectory)
            throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("filters", objectMapper.writeValueAsString(filters));
        conf.set("aggregators", objectMapper.writeValueAsString(aggregators));

        final String jobId = UUID.randomUUID().toString().replace("-", "");
        final Job job = Job.getInstance(conf, jobId);
        FileInputFormat.addInputPath(job, inputDirectory);
        final Path outputDirectoryForCurrentJob = new Path(outputDirectory, jobId);
        FileOutputFormat.setOutputPath(job, outputDirectoryForCurrentJob);

        job.setMapperClass(DataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleOutputs.addNamedOutput(job, jobId, RawDataOutputFormat.class, NullWritable.class, Text.class);

        if (job.waitForCompletion(true)) {
            renameGeneratedFiles(outputDirectoryForCurrentJob, job, conf);
        }
        return jobId;
    }

    private static void renameGeneratedFiles(Path outputDirectory, Job job, Configuration configuration) throws IOException {
        final FileSystem fileSystem = FileSystem.get(configuration);

        final Path filteredResults = new Path(outputDirectory, String.format("%s-m-00000.txt", job.getJobName()));
        if (fileSystem.exists(filteredResults)) {
            final Path renamedFilteredResultsFile = new Path(outputDirectory, "filtered.txt");
            fileSystem.rename(filteredResults, renamedFilteredResultsFile);
        }

        final Path aggregatedResults = new Path(outputDirectory, "part-r-00000");
        if (fileSystem.exists(aggregatedResults)) {
            final Path renamedAggregatedResults = new Path(outputDirectory, "aggregated.txt");
            fileSystem.rename(aggregatedResults, renamedAggregatedResults);
        }
    }
}
