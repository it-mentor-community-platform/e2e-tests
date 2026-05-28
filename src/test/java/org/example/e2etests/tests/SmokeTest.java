package org.example.e2etests.tests;

import org.example.e2etests.tests.base.E2eTestBase;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SmokeTest extends E2eTestBase {

    @Test
    void infrastructureContainersAreRunning() {
        assertThat(postgres.isRunning()).as("PostgreSQL").isTrue();
        assertThat(kafka.isRunning()).as("Kafka").isTrue();
    }

    @Test
    void serviceContainersAreRunning() {
        assertThat(gateway.isRunning()).as("gateway").isTrue();
        assertThat(authService.isRunning()).as("auth-service").isTrue();
        assertThat(dataImporter.isRunning()).as("data-importer").isTrue();
        assertThat(profileService.isRunning()).as("profile-service").isTrue();
        assertThat(projectService.isRunning()).as("project-service").isTrue();
        assertThat(mentorService.isRunning()).as("mentor-service").isTrue();
        assertThat(jobMarketAnalytics.isRunning()).as("job-market-analytics-service").isTrue();
        assertThat(telegramBotAdapter.isRunning()).as("telegram-bot-adapter").isTrue();
    }
}
