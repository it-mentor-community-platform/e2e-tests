package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MentorImportMessageDto(
        @JsonProperty("mentors") List<MentorDto> mentors
) {}