package org.example.e2etests.tests.data;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.example.e2etests.dto.DataImportRequest;
import org.example.e2etests.tests.base.E2eTestBase;
import org.example.e2etests.util.JwtTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

import static org.example.e2etests.dto.DataImportRequest.DataImportType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.util.HttpHeadersTestUtils.createHeaders;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class ReviewImporterE2eTest extends E2eTestBase {

    private static final String API_IMPORT = "/api/data-importer/import";
    private static final String API_IMPORT_STATUS = "/api/data-importer/imports/";

    private static final Duration IMPORT_TIMEOUT_ONE_MINUTE = Duration.ofSeconds(60);
    private static final Duration IMPORT_TIMEOUT_TWO_MINUTES = Duration.ofSeconds(120);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private static final Set<String> TABLE_TO_TRUNCATE = Set.of(
            AUTH_SERVICE_USERS_TABLE,
            MENTOR_SERVICE_MENTORS_TABLE,
            MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE,
            PROFILE_SERVICE_PROFILES_TABLE,
            PROFILE_SERVICE_PROFILES_DETAILS_TABLE,
            PROFILE_SERVICE_PROJECT_TABLE,
            PROJECT_SERVICE_PROJECTS_TABLE,
            PROJECT_SERVICE_REVIEWS_TABLE
    );

    @BeforeEach
    public void setup() {
        truncateTables(TABLE_TO_TRUNCATE);

        assertTableIsEmpty(AUTH_SERVICE_USERS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_DETAILS_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROJECT_TABLE);
        assertTableIsEmpty(PROJECT_SERVICE_PROJECTS_TABLE);
        assertTableIsEmpty(PROJECT_SERVICE_REVIEWS_TABLE);
    }

    @Test
    public void shouldImportProjectReviews() {
        Integer beforeCountRowsInTable = JdbcTestUtils.countRowsInTable(jdbcTemplate, PROJECT_SERVICE_REVIEWS_TABLE);
        assertEquals(0, beforeCountRowsInTable);

        importByType(DataImportType.USERS);
        importByType(DataImportType.PROFILES);
        importByType(DataImportType.MENTORS);
        importByType(DataImportType.PROJECTS);
        importByType(DataImportType.REVIEWS);

        await().atMost(IMPORT_TIMEOUT_ONE_MINUTE).pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        assertThat(JdbcTestUtils.countRowsInTable(jdbcTemplate, PROJECT_SERVICE_REVIEWS_TABLE))
                                .isGreaterThan(0));
        Integer finalCount = JdbcTestUtils.countRowsInTable(jdbcTemplate, PROJECT_SERVICE_REVIEWS_TABLE);
        assertThat(finalCount).isGreaterThan(0);
    }

    private void importByType(DataImportType type) {
        long id = startImport(type);

        await().atMost(IMPORT_TIMEOUT_TWO_MINUTES).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> {
                    String status = getImportStatus(id);
                    if (status.equals("FAILED")) {
                        throw new IllegalStateException("Import failed for id: "+ id);
                    }
                    assertThat(status).isEqualTo("SUCCESS");
                });
    }

    private Long startImport(DataImportType type) {
        return Objects.requireNonNull(testRestTemplate.postForEntity(
                API_IMPORT,
                new HttpEntity<>(
                        new DataImportRequest(type),
                        createHeaders(JwtTestUtils.getAdminJWT(jwtSecret))
                ),
                JsonNode.class
        ).getBody()).path("id").asLong();
    }

    private String getImportStatus(Long id) {
        return Objects.requireNonNull(testRestTemplate.exchange(
                API_IMPORT_STATUS + id,
                HttpMethod.GET,
                new HttpEntity<>(
                        createHeaders(JwtTestUtils.getAdminJWT(jwtSecret))
                ),
                JsonNode.class
        ).getBody()).path("status").asText();
    }
}