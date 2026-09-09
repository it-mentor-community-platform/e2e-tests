package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TaskPayloadDto(
        @JsonProperty("reviewer_telegram_user_id") Long reviewerTelegramUserId,
        String url,
        ProjectDto project,
        List<MentorDto> mentors
) {
}
