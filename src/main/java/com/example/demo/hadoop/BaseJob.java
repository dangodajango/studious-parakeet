package com.example.demo.hadoop;

import com.example.demo.hadoop.filter.FilterDTO;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Component
public class BaseJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${hdfs.filter.file-name}")
    private String filterFileName;

    @Value("${hdfs.aggregation.file-name.average-smoking-prevalence}")
    private String averageSmokingPrevalenceFileName;

    @Value("${hdfs.aggregation.file-name.percentage-access-to-counseling}")
    private String percentageAccessToCounselingFileName;

    public String executeJob(List<FilterDTO> filterDTOs, Path inputDirectory, Path outputDirectory)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        conf.set("filters", objectMapper.writeValueAsString(filterDTOs));

        String jobId = UUID.randomUUID().toString().replace("-", "");
        Job job = Job.getInstance(conf, jobId);
        FileInputFormat.addInputPath(job, inputDirectory);
        Path outputDirectoryForCurrentJob = new Path(outputDirectory, jobId);
        FileOutputFormat.setOutputPath(job, outputDirectoryForCurrentJob);

        job.setMapperClass(BaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(BaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleOutputs.addNamedOutput(job, jobId, RawDataOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "averageSmokingPrevalence", TextOutputFormat.class, Text.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "percentageAccessToCounseling", TextOutputFormat.class, Text.class, DoubleWritable.class);

        if (job.waitForCompletion(true)) {
            renameGeneratedFiles(outputDirectoryForCurrentJob, job, conf);
        }
        return jobId;
    }

    private void renameGeneratedFiles(Path outputDirectory, Job job, Configuration configuration) throws IOException {
        final FileSystem fileSystem = FileSystem.get(configuration);

        final Path filteredResults = new Path(outputDirectory, String.format("%s-m-00000.txt", job.getJobName()));
        if (fileSystem.exists(filteredResults)) {
            final Path renamedFilteredResultsFile = new Path(outputDirectory, filterFileName);
            fileSystem.rename(filteredResults, renamedFilteredResultsFile);
        }

        final Path averageSmokingPrevalenceResults = new Path(outputDirectory, "averageSmokingPrevalence-r-00000");
        if (fileSystem.exists(averageSmokingPrevalenceResults)) {
            final Path renamedAverageSmokingPrevalenceResults = new Path(outputDirectory, averageSmokingPrevalenceFileName);
            fileSystem.rename(averageSmokingPrevalenceResults, renamedAverageSmokingPrevalenceResults);
        }

        final Path percentageAccessToCounselingResults = new Path(outputDirectory, "percentageAccessToCounseling-r-00000");
        if (fileSystem.exists(percentageAccessToCounselingResults)) {
            final Path renamedPercentageAccessToCounselingResults = new Path(outputDirectory, percentageAccessToCounselingFileName);
            fileSystem.rename(percentageAccessToCounselingResults, renamedPercentageAccessToCounselingResults);
        }
    }
}
