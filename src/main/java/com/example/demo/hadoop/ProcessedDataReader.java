package com.example.demo.hadoop;

import com.example.demo.hadoop.model.AggregatedEntry;
import com.example.demo.hadoop.model.FilteredEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Component
public class ProcessedDataReader {

    private final FileSystem fileSystem;

    private final Path outputDirectoryPath;

    public ProcessedDataReader(@Value("${hdfs.file-system.path}") String hdfsFileSystemPath,
                               @Value("${hdfs.output-directory.path}") String outputDirectoryPath) throws IOException {
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(hdfsFileSystemPath), configuration);
        this.outputDirectoryPath = new Path(outputDirectoryPath);
    }

    public List<FilteredEntry> readResultsFromFiltering(String jobId, int startLine, int endLine) throws IOException {
        Path outputDirectoryForJob = new Path(outputDirectoryPath, jobId);
        if (!fileSystem.exists(outputDirectoryForJob)) {
            return Collections.emptyList();
        }

        Path fileContainingFilteredResults = new Path(outputDirectoryForJob, "filtered.txt");
        if (!fileSystem.exists(fileContainingFilteredResults) || isFileEmpty(fileContainingFilteredResults)) {
            return Collections.emptyList();
        }

        return readResultsFromFiltering(fileContainingFilteredResults, startLine, endLine);
    }

    private List<FilteredEntry> readResultsFromFiltering(Path filePath, int startLine, int endLine) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(filePath);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            List<FilteredEntry> filteredEntries = new ArrayList<>();
            int currentLine = 1;
            String filteredEntry;
            while ((filteredEntry = reader.readLine()) != null) {
                if (currentLine > endLine) {
                    break;
                }
                if (currentLine >= startLine) {
                    filteredEntries.add(FilteredEntry.mapToFilteredEntry(filteredEntry));
                }
                currentLine++;
            }
            return filteredEntries;
        }
    }

    public List<List<AggregatedEntry>> readResultsFromAggregations(String jobId) throws IOException {
        Path outputDirectoryForJob = new Path(outputDirectoryPath, jobId);
        if (!fileSystem.exists(outputDirectoryForJob)) {
            return Collections.emptyList();
        }
        return readResultsFromAggregations(outputDirectoryForJob);
    }

    private List<List<AggregatedEntry>> readResultsFromAggregations(Path outputDirectoryPath) throws IOException {
        List<List<AggregatedEntry>> resultsFromAllAggregations = new ArrayList<>();
        FileStatus[] fileStatuses = fileSystem.listStatus(outputDirectoryPath);
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isFile() || !isAggregationFile(fileStatus)) {
                continue;
            }
            FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                List<AggregatedEntry> singleAggregationResults = new ArrayList<>();
                String aggregatedEntry;
                while ((aggregatedEntry = reader.readLine()) != null) {
                    singleAggregationResults.add(AggregatedEntry.mapToAggregatedEntry(aggregatedEntry));
                }
                resultsFromAllAggregations.add(singleAggregationResults);
            }
        }
        return resultsFromAllAggregations;
    }

    private boolean isFileEmpty(Path path) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        return fileStatus.getLen() == 0;
    }

    private boolean isAggregationFile(FileStatus fileStatus) {
        List<String> validFileNames = Arrays.asList("averageSmokingPrevalence.txt", "percentageAccessToCounseling.txt");
        return validFileNames.contains(fileStatus.getPath().getName());
    }
}
