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
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(hdfsFileSystemPath), configuration);
        this.outputDirectoryPath = new Path(outputDirectoryPath);
    }

    public List<FilteredEntry> readFilteredResults(String jobId) throws IOException {
        Path outputDirectoryForJob = new Path(outputDirectoryPath, jobId);
        if (!fileSystem.exists(outputDirectoryForJob)) {
            return Collections.emptyList();
        }

        Path filteredResultsPath = new Path(outputDirectoryForJob, "filtered.txt");
        if (!fileSystem.exists(filteredResultsPath) || isFileEmpty(filteredResultsPath)) {
            return Collections.emptyList();
        }

        return mapper(filteredResultsPath);
    }

    private List<FilteredEntry> mapper(Path filePath) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(filePath);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            List<FilteredEntry> filteredEntries = new ArrayList<>();
            String filteredEntry;
            while ((filteredEntry = reader.readLine()) != null) {
                filteredEntries.add(FilteredEntry.mapToFilteredEntry(filteredEntry));
            }
            return filteredEntries;
        }
    }

    public List<AggregatedEntry> readAggregatedResults(String jobId) throws IOException {
        Path outputDirectoryForJob = new Path(outputDirectoryPath, jobId);
        if (!fileSystem.exists(outputDirectoryForJob)) {
            return Collections.emptyList();
        }

        Path aggregatedResultsPath = new Path(outputDirectoryForJob, "aggregated.txt");
        if (!fileSystem.exists(aggregatedResultsPath) || isFileEmpty(aggregatedResultsPath)) {
            return Collections.emptyList();
        }

        return Collections.emptyList();
    }

    private boolean isFileEmpty(Path path) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        return fileStatus.getLen() == 0;
    }
}
