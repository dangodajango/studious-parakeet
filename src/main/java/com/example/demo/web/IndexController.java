package com.example.demo.web;

import com.example.demo.hadoop.model.Aggregator;
import com.example.demo.hadoop.model.Filter;
import com.example.demo.hadoop.model.FilterConfiguration;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;

import static com.example.demo.hadoop.model.Aggregator.AVERAGE_SMOKING_PREVALENCE;
import static com.example.demo.hadoop.model.Aggregator.PERCENTAGE_ACCESS_TO_COUNSELING;

@Controller
@RequiredArgsConstructor
public class IndexController {

    private final IndexService indexService;

    @GetMapping("/")
    public String index(@RequestParam(defaultValue = "1") int lineNumber, Model model)
            throws IOException, InterruptedException, ClassNotFoundException {
        Filter[] selectedFilters = new Filter[]{new Filter(FilterConfiguration.GENDER, "\"Both\"")};
        Aggregator[] selectedAggregators = new Aggregator[]{AVERAGE_SMOKING_PREVALENCE, PERCENTAGE_ACCESS_TO_COUNSELING};

        Response response = indexService.processRequest(selectedFilters, selectedAggregators, lineNumber);

        model.addAttribute("filteredEntries", response.getFilteredEntries());
        model.addAttribute("aggregatedEntries", response.getAggregatedEntries());
        model.addAttribute("lineNumber", lineNumber);
        model.addAttribute("pageSize", indexService.getPageSize());

        return "index";
    }
}



