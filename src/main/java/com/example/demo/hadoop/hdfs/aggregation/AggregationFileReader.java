package com.example.demo.hadoop.hdfs.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class AggregationFileReader {

    private final FileSystem fileSystem;

    public AggregationFileReader() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        fileSystem = FileSystem.get(configuration);
    }

    public AggregationFileContent readFile(Path outputDirectoryForCurrentJob, String fileName) throws IOException {
        Path pathToFile = new Path(outputDirectoryForCurrentJob, fileName);
        if (!fileSystem.exists(pathToFile)) {
            return new AggregationFileContent("", Collections.emptyList());
        }
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(pathToFile)))) {
            List<AggregationFileRow> rows = new ArrayList<>();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] c = line.split("\t");
                String[] x = c[0].split(",");
                rows.add(new AggregationFileRow(x, c[1]));
            }
            return new AggregationFileContent(fileName, rows);
        }
    }
}
