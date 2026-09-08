package org.example.e2etests.tests.data;

import lombok.extern.slf4j.Slf4j;
import org.example.e2etests.tests.base.E2eTestBase;
import org.example.e2etests.util.JwtTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.util.HttpHeadersTestUtils.createHeaders;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class ReviewImporterE2eTest extends E2eTestBase {

    private static final String API_PROJECT_REVIEW_IMPORT = "/api/data-importer/start-reviews-import";
    private static final String API_USERS_IMPORT = "/api/data-importer/start-users-import";
    private static final String API_PROFILES_IMPORT = "/api/data-importer/start-profiles-import";
    private static final String API_PROJECTS_IMPORT = "/api/data-importer/start-projects-import";
    private static final String API_MENTORS_IMPORT = "/api/data-importer/start-mentors-import";

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
        startImportByAPI(API_USERS_IMPORT, IMPORT_TIMEOUT_ONE_MINUTE, jdbcTemplate, AUTH_SERVICE_USERS_TABLE);
        startImportByAPI(API_PROFILES_IMPORT, IMPORT_TIMEOUT_ONE_MINUTE, jdbcTemplate, PROFILE_SERVICE_PROFILES_TABLE);
        startImportByAPI(API_MENTORS_IMPORT, IMPORT_TIMEOUT_TWO_MINUTES, jdbcTemplate, MENTOR_SERVICE_MENTORS_TABLE);
        startImportByAPI(API_PROJECTS_IMPORT, IMPORT_TIMEOUT_ONE_MINUTE, jdbcTemplate, PROJECT_SERVICE_PROJECTS_TABLE);

        startImport(API_PROJECT_REVIEW_IMPORT);
        await().atMost(IMPORT_TIMEOUT_ONE_MINUTE).pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        assertThat(JdbcTestUtils.countRowsInTable(jdbcTemplate, PROJECT_SERVICE_REVIEWS_TABLE))
                                .isGreaterThan(0));
        Integer finalCount = JdbcTestUtils.countRowsInTable(jdbcTemplate, PROJECT_SERVICE_REVIEWS_TABLE);
        assertThat(finalCount).isGreaterThan(0);
    }

    private void startImportByAPI(String apiName, Duration timeout, JdbcTemplate jdbcTemplate, String tableName) {
        startImport(apiName);
        await().atMost(timeout).pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        assertThat(JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName))
                                .isGreaterThan(0));
    }

    private void startImport(String path) {
        String jwtToken = JwtTestUtils.getAdminJWT(jwtSecret);
        testRestTemplate.postForEntity(
                path,
                new HttpEntity<>(createHeaders(jwtToken)),
                String.class
        );
    }
}