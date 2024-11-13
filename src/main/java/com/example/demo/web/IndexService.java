package com.example.demo.web;

import com.example.demo.hadoop.BaseJob;
import com.example.demo.hadoop.ProcessedDataReader;
import com.example.demo.hadoop.model.AggregatedEntry;
import com.example.demo.hadoop.model.FilteredEntry;
import com.example.demo.hadoop.filter.FilterDTO;
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

    private final BaseJob dataProcessingJob;

    private final ProcessedDataReader processedDataReader;

    @Value("${hdfs.input-directory.path}")
    private String inputDirectoryPath;

    @Value("${hdfs.output-directory.path}")
    private String outputDirectoryPath;

    @Value("${page.size}")
    @Getter
    private int pageSize;

    public Response processRequest(List<FilterDTO> filterDTOs, int lineNumber) throws IOException, InterruptedException, ClassNotFoundException {
        String jobId = dataProcessingJob.executeJob(filterDTOs, new Path(inputDirectoryPath), new Path(outputDirectoryPath));
        List<FilteredEntry> filteredEntries = processedDataReader.readFilteredResults(jobId, lineNumber, lineNumber + 30);
        List<AggregatedEntry> aggregatedEntries = processedDataReader.readAggregatedResults(jobId);
        return new Response(filteredEntries, aggregatedEntries);
    }
}
