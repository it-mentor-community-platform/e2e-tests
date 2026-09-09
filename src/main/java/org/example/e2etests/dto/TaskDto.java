package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TaskDto(
        String taskType,
        TaskPayloadDto payload
) {
}
