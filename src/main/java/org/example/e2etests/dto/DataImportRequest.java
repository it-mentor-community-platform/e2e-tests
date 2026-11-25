package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DataImportRequest(
        @JsonProperty("type")
        DataImportType type
) {
    public enum DataImportType {
        USERS,
        PROFILES,
        MENTORS,
        PROJECTS,
        REVIEWS,
        GUARANTEED_REVIEWS,
        QUESTIONS
    }
}