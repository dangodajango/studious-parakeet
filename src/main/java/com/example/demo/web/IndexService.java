package com.example.demo.web;

import com.example.demo.hadoop.DataProcessingJob;
import com.example.demo.hadoop.ProcessedDataReader;
import com.example.demo.hadoop.model.AggregatedEntry;
import com.example.demo.hadoop.model.Aggregator;
import com.example.demo.hadoop.model.Filter;
import com.example.demo.hadoop.model.FilteredEntry;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@RequiredArgsConstructor
public class IndexService {

    private final DataProcessingJob dataProcessingJob;

    private final ProcessedDataReader processedDataReader;

    @Value("${hdfs.input-directory.path}")
    private String inputDirectoryPath;

    @Value("${hdfs.output-directory.path}")
    private String outputDirectoryPath;

    @Value("${page.size}")
    @Getter
    private int pageSize;

    public Response processRequest(Filter[] filters, Aggregator[] aggregators, int lineNumber) throws IOException, InterruptedException, ClassNotFoundException {
        String jobId = dataProcessingJob.executeJob(filters, aggregators,
                new Path(inputDirectoryPath), new Path(outputDirectoryPath));
        List<FilteredEntry> filteredEntries = processedDataReader.readFilteredResults(jobId, lineNumber, lineNumber + 30);
        List<AggregatedEntry> aggregatedEntries = processedDataReader.readAggregatedResults(jobId);
        return new Response(filteredEntries, aggregatedEntries);
    }
}
