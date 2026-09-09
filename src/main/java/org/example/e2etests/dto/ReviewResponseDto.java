package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ReviewResponseDto(
        @JsonProperty("reviewer_telegram_user_id") long reviewerTelegramUserId,
        @JsonProperty("url") String url,
        @JsonProperty("project") ProjectDto project
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProjectDto(
            @JsonProperty("github_repository_url") String githubRepositoryUrl
    ) {}
}