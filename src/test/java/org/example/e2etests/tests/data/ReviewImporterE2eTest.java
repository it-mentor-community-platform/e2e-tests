package org.example.e2etests.tests.data;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.example.e2etests.tests.base.E2eTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

import javax.crypto.SecretKey;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.HttpHeadersTestUtils.createHeaders;
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
    private static final Duration SLEEP_TEN_SECONDS_INTERVAL = Duration.ofMillis(10_000);

    @BeforeEach
    public void setup() {
        clearTables();
    }

    @AfterEach
    public void tearDown() {
        clearTables();
    }

    @Test
    public void when() {
        Integer beforeCountRowsInTable = getCountRowsInTable(PROJECT_SERVICE_REVIEWS_TABLE);
        assertEquals(0, beforeCountRowsInTable);
        startImportByAPI(API_USERS_IMPORT, IMPORT_TIMEOUT_ONE_MINUTE, jdbcTemplate, AUTH_SERVICE_USERS_TABLE);
        startImportByAPI(API_PROFILES_IMPORT, IMPORT_TIMEOUT_ONE_MINUTE, jdbcTemplate, PROFILE_SERVICE_PROFILES_TABLE);
        startImportByAPI(API_MENTORS_IMPORT, IMPORT_TIMEOUT_TWO_MINUTES, jdbcTemplate, MENTOR_SERVICE_MENTORS_TABLE);
        startImportByAPI(API_PROJECTS_IMPORT, IMPORT_TIMEOUT_ONE_MINUTE, jdbcTemplate, PROJECT_SERVICE_PROJECTS_TABLE);
        Integer totalProjects = getCountRowsInTable(PROJECT_SERVICE_PROJECTS_TABLE);
        assertThat(totalProjects).isGreaterThan(0);
        startImport(API_PROJECT_REVIEW_IMPORT);
        try {
            Thread.sleep(SLEEP_TEN_SECONDS_INTERVAL);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Integer finalCount = getCountRowsInTable(PROJECT_SERVICE_REVIEWS_TABLE);
        assertThat(finalCount).isGreaterThan(0);
    }

    private void startImportByAPI(String apiName, Duration timeout, JdbcTemplate jdbcTemplate, String tableName) {
        startImport(apiName);
        await().atMost(timeout).pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        assertThat(JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName))
                                .isGreaterThan(0));
    }

    private int getCountRowsInTable(String tableName) {
        return JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName);
    }

    private void startImport(String path) {
        testRestTemplate.postForEntity(
                path,
                new HttpEntity<>(createHeaders(getAdminJWT())),
                String.class
        );
    }

    private void clearTables() {
        jdbcTemplate.execute("TRUNCATE TABLE auth_service.users RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE mentor_service.mentors RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE mentor_service.guaranteed_reviews_prices RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE profile_service.profiles RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE profile_service.profiles_details RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE profile_service.project RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE project_service.projects RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE project_service.reviews RESTART IDENTITY CASCADE");
    }

    private String getAdminJWT() {
        SecretKey key = Keys.hmacShaKeyFor(Base64.getDecoder().decode(jwtSecret));
        Date now = new Date();
        return Jwts.builder()
                .subject("review_test")
                .claim("roles", List.of("ADMIN"))
                .claim("telegram_username", "e2e_test_admin")
                .issuedAt(now)
                .expiration(new Date(now.getTime() + 3_600_000))
                .signWith(key)
                .compact();
    }
}