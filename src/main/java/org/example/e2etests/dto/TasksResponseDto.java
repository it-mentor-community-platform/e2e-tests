package org.example.e2etests.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TasksResponseDto(
        List<TaskDto> tasks
) {
}
