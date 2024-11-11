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
import java.util.Collections;
import java.util.List;

@Component
public class ProcessedDataReader {

    private final FileSystem fileSystem;

    private final Path outputDirectoryPath;

    public ProcessedDataReader(@Value("${hdfs.file-system.path}") String hdfsFileSystemPath,
                               @Value("${hdfs.output-directory.path}") String outputDirectoryPath) throws IOException {
        final Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(hdfsFileSystemPath), configuration);
        this.outputDirectoryPath = new Path(outputDirectoryPath);
    }

    public List<FilteredEntry> readFilteredResults(final String jobId, final int startLine, final int endLine) throws IOException {
        final Path outputDirectoryForJob = new Path(outputDirectoryPath, jobId);
        if (!fileSystem.exists(outputDirectoryForJob)) {
            return Collections.emptyList();
        }

        final Path filteredResultsPath = new Path(outputDirectoryForJob, "filtered.txt");
        if (!fileSystem.exists(filteredResultsPath) || isFileEmpty(filteredResultsPath)) {
            return Collections.emptyList();
        }

        return readFilteredResults(filteredResultsPath, startLine, endLine);
    }

    private List<FilteredEntry> readFilteredResults(final Path filePath, final int startLine, final int endLine) throws IOException {
        final FSDataInputStream inputStream = fileSystem.open(filePath);
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            final List<FilteredEntry> filteredEntries = new ArrayList<>();
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

    public List<AggregatedEntry> readAggregatedResults(final String jobId) throws IOException {
        final Path outputDirectoryForJob = new Path(outputDirectoryPath, jobId);
        if (!fileSystem.exists(outputDirectoryForJob)) {
            return Collections.emptyList();
        }

        final Path aggregatedResultsPath = new Path(outputDirectoryForJob, "aggregated.txt");
        if (!fileSystem.exists(aggregatedResultsPath) || isFileEmpty(aggregatedResultsPath)) {
            return Collections.emptyList();
        }

        return readAggregatedResults(aggregatedResultsPath);
    }

    private List<AggregatedEntry> readAggregatedResults(final Path filePath) throws IOException {
        final FSDataInputStream inputStream = fileSystem.open(filePath);
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            final List<AggregatedEntry> aggregatedEntries = new ArrayList<>();
            String aggregatedEntry;
            while ((aggregatedEntry = reader.readLine()) != null) {
                aggregatedEntries.add(AggregatedEntry.mapToAggregatedEntry(aggregatedEntry));
            }
            return aggregatedEntries;
        }
    }

    private boolean isFileEmpty(final Path path) throws IOException {
        final FileStatus fileStatus = fileSystem.getFileStatus(path);
        return fileStatus.getLen() == 0;
    }
}
