package com.example.demo.web;

import com.example.demo.hadoop.filter.AgeGroupFilter;
import com.example.demo.hadoop.filter.FilterDTO;
import com.example.demo.hadoop.filter.GenderFilter;
import com.example.demo.hadoop.filter.PeerInfluenceFilter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.Arrays;

@Controller
@RequiredArgsConstructor
public class IndexController {

    private final IndexService indexService;

    @GetMapping("/")
    public String index(@RequestParam(defaultValue = "1") int lineNumber, Model model)
            throws IOException, InterruptedException, ClassNotFoundException {
        FilterDTO genderFilterDTO = new FilterDTO(GenderFilter.FILTER_NAME, Arrays.asList("Both", "Male"));
        FilterDTO peerInfluenceDTO = new FilterDTO(PeerInfluenceFilter.FILTER_NAME, Arrays.asList("1", "2", "3", "4", "5"));
        FilterDTO ageGroupDTO = new FilterDTO(AgeGroupFilter.FILTER_NAME, Arrays.asList("1", "50"));

        Response response = indexService.processRequest(Arrays.asList(genderFilterDTO, peerInfluenceDTO, ageGroupDTO), lineNumber);

        model.addAttribute("filteredEntries", response.getResultsFromFiltering());
        model.addAttribute("resultsFromAggregations", response.getResultsFromAggregations());
        model.addAttribute("lineNumber", lineNumber);
        model.addAttribute("pageSize", indexService.getPageSize());

        return "index";
    }
}



