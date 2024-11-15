package com.example.demo.hadoop.hdfs.filter;

import org.apache.hadoop.conf.Configuration;
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
public class FilterFileReader {

    private final FileSystem fileSystem;

    public FilterFileReader(@Value("${hdfs.file-system.path}") String hdfsFileSystemPath) throws IOException {
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(hdfsFileSystemPath), configuration);
    }

    public FilterFileContent readFile(Path outputDirectoryForCurrentJob, String fileName, int startLine, int endLine) throws IOException {
        Path pathToFile = new Path(outputDirectoryForCurrentJob, fileName);
        if (!fileSystem.exists(pathToFile)) {
            return new FilterFileContent(Collections.emptyList());
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(pathToFile)))) {
            List<FilterFileRow> rows = new ArrayList<>();
            int currentLine = 1;
            String line;
            while ((line = reader.readLine()) != null) {
                if (currentLine > endLine) {
                    break;
                }
                if (currentLine >= startLine) {
                    rows.add(FilterFileRow.mapToFileRow(line));
                }
                currentLine++;
            }
            return new FilterFileContent(rows);
        }
    }
}
