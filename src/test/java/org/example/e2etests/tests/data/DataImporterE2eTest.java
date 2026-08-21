package org.example.e2etests.tests.data;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.awaitility.core.ThrowingRunnable;
import org.example.e2etests.tests.base.E2eTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.test.jdbc.JdbcTestUtils;

import javax.crypto.SecretKey;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.HttpHeadersTestUtils.createHeaders;

@Slf4j
public class DataImporterE2eTest extends E2eTestBase {

    private static final String API_PROFILES_IMPORT = "/api/data-importer/start-profiles-import";
    private static final String API_PROJECTS_IMPORT = "/api/data-importer/start-projects-import";
    private static final String API_USERS_IMPORT = "/api/data-importer/start-users-import";
    private static final String API_MENTORS_IMPORT = "/api/data-importer/start-mentors-import";
    private static final String API_GUARANTEED_REVIEWS_IMPORT = "/api/data-importer/start-guaranteed-reviews-import";

    private static final String UPDATE_DESCRIPTION_MENTOR_SQL = "UPDATE " + MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE + " SET cost = ? WHERE name = ?";
    private static final String GET_DESCRIPTION_MENTOR_SQL = "SELECT cost FROM " + MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE + " WHERE name = ?";


    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE auth_service.users RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE profile_service.profiles RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE project_service.projects RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE mentor_service.guaranteed_reviews_prices RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE mentor_service.mentors RESTART IDENTITY CASCADE");
    }

    @Test
    void shouldImportUsersAndSendMessagesToKafka() {
        assertTableIsEmpty(AUTH_SERVICE_USERS_TABLE);
        kafkaConsumer.subscribe(List.of(AUTH_USER_CREATED_TOPIC));

        startImport(API_USERS_IMPORT);

        executeWithAwait(60, () -> {
                    assertTableHasRecords(AUTH_SERVICE_USERS_TABLE);
                    assertTableHasRecords(AUTH_SERVICE_USERS_ROLES_TABLE);
                }
        );

        executeWithAwait(60, () -> {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            assertThat(records.count()).isGreaterThan(0);
            assertThat(records.iterator().next().value()).contains("telegram_user_id");
        });
    }

    @Test
    void shouldImportProfilesAndProjects() {
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_TABLE);

        startImport(API_PROFILES_IMPORT);

        executeWithAwait(60, () ->
                assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE)
        );

        assertTableIsEmpty(PROJECT_SERVICE_PROJECTS_TABLE);
        kafkaConsumer.subscribe(List.of(PROJECTS_PROJECT_CREATED_TOPIC));
        startImport(API_PROJECTS_IMPORT);

        executeWithAwait(60, () ->
                assertTableHasRecords(PROJECT_SERVICE_PROJECTS_TABLE)
        );

        executeWithAwait(60, () -> {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            assertThat(records.count()).isGreaterThan(0);
            assertThat(records.iterator().next().value()).contains("author_telegram_user_id");
        });
    }

    @Test
    void shouldImportMentorsAndReviewPricesAndSendMessagesToKafka() {
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);

        kafkaConsumer.subscribe(List.of(NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC));

        startImport(API_PROFILES_IMPORT);
        executeWithAwait(60,
                () -> {
                    assertTableHasRecords(PROFILE_SERVICE_PROJECT_TABLE);
                    assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE);
                }
        );

        startImport(API_MENTORS_IMPORT);
        executeWithAwait(200, () -> {
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_SERVICES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_SERVICES_TABLE);
                }
        );

        startImport(API_GUARANTEED_REVIEWS_IMPORT);
        executeWithAwait(60, () ->
                assertTableHasRecords(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE)
        );


        executeWithAwait(60, () -> {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            assertThat(records.count()).isGreaterThan(0);
            assertThat(records.iterator().next().value()).contains("author_telegram_user_id");
        });
    }

    @Test
    void shouldNotOverwriteMentorDescription() {
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);

        startImport(API_PROFILES_IMPORT);
        executeWithAwait(60,
                () -> {
                    assertTableHasRecords(PROFILE_SERVICE_PROJECT_TABLE);
                    assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE);
                }
        );

        startImport(API_MENTORS_IMPORT);
        executeWithAwait(200, () -> {
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_SERVICES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_SERVICES_TABLE);
                }
        );

        String description = "Бесплатно всем";
        String testName = "Артём";
        jdbcTemplate.update(
                UPDATE_DESCRIPTION_MENTOR_SQL,
                description, testName
        );

        startImport(API_MENTORS_IMPORT);
        executeWithAwait(30, () ->
                assertTableHasRecords(MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE)
        );

        String descriptionAfter = jdbcTemplate.queryForObject(
                GET_DESCRIPTION_MENTOR_SQL,
                String.class, testName
        );

        assertThat(descriptionAfter).isEqualTo(description);

    }


    private void executeWithAwait(int second, ThrowingRunnable throwingRunnable) {

        await()
                .atMost(Duration.ofSeconds(second))
                .pollInterval(Duration.ofSeconds(3))
                .untilAsserted(throwingRunnable);
    }

    private void assertTableIsEmpty(String tableName) {
        int count = JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName);
        assertThat(count).isZero();
    }

    private String createAdminToken() {
        SecretKey key = Keys.hmacShaKeyFor(Base64.getDecoder().decode(jwtSecret));
        Date now = new Date();
        return Jwts.builder()
                .subject("123456789")
                .claim("roles", List.of("ADMIN"))
                .claim("telegram_username", "e2e_test_admin")
                .issuedAt(now)
                .expiration(new Date(now.getTime() + 3_600_000))
                .signWith(key)
                .compact();
    }

    private void assertTableHasRecords(String tableName) {
        int count = JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName);
        assertThat(count).isGreaterThan(0);
    }

    private void startImport(String path) {
        testRestTemplate.postForEntity(
                path,
                new HttpEntity<>(createHeaders(createAdminToken())),
                String.class
        );
    }
}
