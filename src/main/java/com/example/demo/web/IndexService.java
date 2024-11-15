package com.example.demo.web;

import com.example.demo.hadoop.BaseJob;
import com.example.demo.hadoop.filter.FilterDTO;
import com.example.demo.hadoop.hdfs.aggregation.AggregationFileContent;
import com.example.demo.hadoop.hdfs.aggregation.AggregationFileReader;
import com.example.demo.hadoop.hdfs.filter.FilterFileContent;
import com.example.demo.hadoop.hdfs.filter.FilterFileReader;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Service
@RequiredArgsConstructor
public class IndexService {

    private final BaseJob dataProcessingJob;

    private final FilterFileReader filterFileReader;

    private final AggregationFileReader aggregationFileReader;

    @Value("${hdfs.input-directory.path}")
    private String inputDirectoryPath;

    @Value("${hdfs.output-directory.path}")
    private String outputDirectoryPath;

    @Value("${hdfs.filter.file-name}")
    private String filterFileName;

    @Value("${hdfs.aggregation.file-name.average-smoking-prevalence}")
    private String averageSmokingPrevalenceFileName;

    @Value("${hdfs.aggregation.file-name.percentage-access-to-counseling}")
    private String percentageAccessToCounselingFileName;

    @Value("${page.size}")
    @Getter
    private int pageSize;

    public Response processRequest(List<FilterDTO> filterDTOs, int lineNumber) throws IOException, InterruptedException, ClassNotFoundException {
        String jobId = dataProcessingJob.executeJob(filterDTOs, new Path(inputDirectoryPath), new Path(outputDirectoryPath));
        Path outputDirectoryPathForCurrentJob = new Path(outputDirectoryPath, jobId);
        FilterFileContent filterFileContent = filterFileReader.readFile(outputDirectoryPathForCurrentJob, filterFileName, lineNumber, lineNumber + pageSize);
        AggregationFileContent averageSmokingPrevalenceContent = aggregationFileReader.readFile(outputDirectoryPathForCurrentJob, averageSmokingPrevalenceFileName);
        AggregationFileContent percentageAccessToCounselingContent = aggregationFileReader.readFile(outputDirectoryPathForCurrentJob, percentageAccessToCounselingFileName);
        return new Response(filterFileContent, Arrays.asList(averageSmokingPrevalenceContent, percentageAccessToCounselingContent));
    }
}
