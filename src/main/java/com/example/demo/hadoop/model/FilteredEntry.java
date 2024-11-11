package com.example.demo.hadoop.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@AllArgsConstructor
@ToString
public class FilteredEntry {
    private int year;
    private String ageGroup;
    private String gender;
    private double smokingPrevalence;
    private double drugExperimentation;
    private String socioeconomicStatus;
    private int peerInfluence;
    private String schoolPrograms;
    private int familyBackground;
    private int mentalHealth;
    private String accessToCounseling;
    private int parentalSupervision;
    private String substanceEducation;
    private int communitySupport;
    private int mediaInfluence;

    public static FilteredEntry mapToFilteredEntry(String filteredEntryString) {
        String[] entryColumns = filteredEntryString.replace("\"", "").split(",");
        return new FilteredEntryBuilder()
                .year(Integer.parseInt(entryColumns[0]))
                .ageGroup(entryColumns[1])
                .gender(entryColumns[2])
                .smokingPrevalence(Double.parseDouble(entryColumns[3]))
                .drugExperimentation(Double.parseDouble(entryColumns[4]))
                .socioeconomicStatus(entryColumns[5])
                .peerInfluence(Integer.parseInt(entryColumns[6]))
                .schoolPrograms(entryColumns[7])
                .familyBackground(Integer.parseInt(entryColumns[8]))
                .mentalHealth(Integer.parseInt(entryColumns[9]))
                .accessToCounseling(entryColumns[10])
                .parentalSupervision(Integer.parseInt(entryColumns[11]))
                .substanceEducation(entryColumns[12])
                .communitySupport(Integer.parseInt(entryColumns[13]))
                .mediaInfluence(Integer.parseInt(entryColumns[14]))
                .build();
    }
}
