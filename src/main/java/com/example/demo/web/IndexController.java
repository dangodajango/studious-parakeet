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
import java.util.List;

@Controller
@RequiredArgsConstructor
public class IndexController {

    private final IndexService indexService;

    @GetMapping("/")
    public String index(@RequestParam(defaultValue = "1") int lineNumber,
                        @RequestParam(defaultValue = "Both, Male, Female") List<String> genders,
                        @RequestParam(defaultValue = "1,2,3,4,5,6,7,8,9,10") List<String> peerInfluence,
                        @RequestParam(defaultValue = "1, 100") List<String> ageGroup,
                        Model model)
            throws IOException, InterruptedException, ClassNotFoundException {
        FilterDTO genderFilterDTO = new FilterDTO(GenderFilter.FILTER_NAME, genders);
        FilterDTO peerInfluenceDTO = new FilterDTO(PeerInfluenceFilter.FILTER_NAME, peerInfluence);
        FilterDTO ageGroupDTO = new FilterDTO(AgeGroupFilter.FILTER_NAME, ageGroup);

        Response response = indexService.processRequest(Arrays.asList(genderFilterDTO, peerInfluenceDTO, ageGroupDTO), lineNumber);

        model.addAttribute("resultsFromFiltering", response.getResultsFromFiltering());
        model.addAttribute("resultsFromAggregations", response.getResultsFromAggregations());
        model.addAttribute("lineNumber", lineNumber);
        model.addAttribute("pageSize", indexService.getPageSize());

        return "index";
    }
}



