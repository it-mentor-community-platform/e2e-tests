package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MentorDto(
        @JsonProperty("mentor_telegram_user_id") long mentorTelegramUserId,
        @JsonProperty("mentor_telegram_profile_url") String mentorTelegramProfileUrl
) {
}
