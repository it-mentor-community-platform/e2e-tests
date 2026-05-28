package org.example.e2etests.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "docker.image.tags")
@Getter
@Setter
class DockerImageTagsProperties {
    private String gateway;
    private String authService;
    private String dataImporter;
    private String profileService;
    private String projectService;
    private String mentorService;
    private String jobMarketAnalyticsService;
    private String telegramBotAdapter;
}
