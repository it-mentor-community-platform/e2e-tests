package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(NON_NULL)
public class CreateReviewRequest {

    @JsonProperty("project_github_repository_url")
    private String projectGithubRepositoryUrl;

    @JsonProperty("review_url")
    private String reviewUrl;
}
