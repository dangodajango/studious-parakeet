package com.example.demo.web;

import com.example.demo.hadoop.DataProcessingJob;
import com.example.demo.hadoop.ProcessedDataReader;
import com.example.demo.hadoop.model.*;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;
import java.util.List;

@Controller
@RequiredArgsConstructor
public class HomeController {

    private final DataProcessingJob dataProcessingJob;

    private final ProcessedDataReader processedDataReader;

    @GetMapping("/")
    public String test(Model model) throws IOException, InterruptedException, ClassNotFoundException {
        Path outputPath = new Path("/output");
        String jobId = dataProcessingJob.executeJob(new Filter[]{new Filter(FilterConfiguration.GENDER, "\"Both\"")},
                new Aggregator[]{},
                new Path("/input"), outputPath);
        List<FilteredEntry> filteredEntries = processedDataReader.readFilteredResults(jobId);
        model.addAttribute("filteredEntries", filteredEntries);

        List<AggregatedEntry> aggregatedEntries = processedDataReader.readAggregatedResults(jobId);
        return "index";
    }
}



