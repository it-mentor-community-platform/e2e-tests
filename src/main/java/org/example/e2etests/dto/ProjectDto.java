package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ProjectDto(
        @JsonProperty("github_repository_url") String githubRepositoryUrl
) {
}
