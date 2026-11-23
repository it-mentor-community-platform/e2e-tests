package org.example.e2etests.tests.data;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ThrowingRunnable;
import org.example.e2etests.dto.DataImportRequest;
import org.example.e2etests.tests.base.E2eTestBase;
import org.example.e2etests.util.JwtTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.example.e2etests.dto.DataImportRequest.DataImportType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.util.HttpHeadersTestUtils.createHeaders;

@Slf4j
public class DataImporterE2eTest extends E2eTestBase {

    private static final String API_IMPORT = "/api/data-importer/import";
    private static final String API_IMPORT_STATUS = "/api/data-importer/imports/";

    private static final Duration IMPORT_TIMEOUT_TWO_MINUTES = Duration.ofSeconds(120);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private static final String UPDATE_DESCRIPTION_MENTOR_SQL = "UPDATE " + MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE + " SET cost = ? WHERE name = ?";
    private static final String GET_DESCRIPTION_MENTOR_SQL = "SELECT cost FROM " + MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE + " WHERE name = ?";
    private static final Set<String> TABLE_TO_TRUNCATE = Set.of(
            AUTH_SERVICE_USERS_TABLE,
            PROFILE_SERVICE_PROFILES_TABLE,
            PROJECT_SERVICE_PROJECTS_TABLE,
            MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE,
            MENTOR_SERVICE_MENTORS_TABLE,
            PROFILE_SERVICE_PROJECT_TABLE
    );

    @BeforeEach
    void setUp() {
        truncateTables(TABLE_TO_TRUNCATE);
    }

    @AfterEach
    void cleanTables() {
        truncateTables(TABLE_TO_TRUNCATE);
    }

    @Test
    void shouldImportUsersAndSendMessagesToKafka() {
        assertTableIsEmpty(AUTH_SERVICE_USERS_TABLE);
        kafkaConsumer.subscribe(List.of(AUTH_USER_CREATED_TOPIC));

        importByType(DataImportType.USERS);

        awaitUsersImported();
        assertAuthUserCreatedPublished();
    }

    @Test
    void shouldImportProfilesAndProjects() {
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_TABLE);

        importByType(DataImportType.PROFILES);

        executeWithAwait(60, () ->
                assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE)
        );

        assertTableIsEmpty(PROJECT_SERVICE_PROJECTS_TABLE);
        kafkaConsumer.subscribe(List.of(PROJECTS_PROJECT_CREATED_TOPIC));
        importByType(DataImportType.PROJECTS);

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
    void shouldImportMentorsAndSendMessagesToKafka() {
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);

        kafkaConsumer.subscribe(List.of(NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC));

        importByType(DataImportType.PROFILES);
        executeWithAwait(60,
                () -> {
                    assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE);
                }
        );

        importByType(DataImportType.PROJECTS);
        executeWithAwait(60, () ->
                assertTableHasRecords(PROFILE_SERVICE_PROJECT_TABLE)
        );

        importByType(DataImportType.MENTORS);
        executeWithAwait(60, () -> {
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_SERVICES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_SERVICES_TABLE);
                }
        );

        importByType(DataImportType.GUARANTEED_REVIEWS);
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
    void shouldImportGuaranteedReviews() {
        assertTableIsEmpty(AUTH_SERVICE_USERS_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);

        kafkaConsumer.subscribe(List.of(AUTH_USER_CREATED_TOPIC));

        importByType(DataImportType.USERS);
        awaitUsersImported();
        assertAuthUserCreatedPublished();

        importByType(DataImportType.PROFILES);
        awaitProfilesImported();

        importByType(DataImportType.MENTORS);
        awaitMentorsImported();
        assertAllMentorsHaveProfiles();

        importByType(DataImportType.GUARANTEED_REVIEWS);
        awaitGuaranteedReviewsImported();
    }

    @Test
    void shouldNotOverwriteMentorDescription() {
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);

        importByType(DataImportType.PROFILES);
        executeWithAwait(60,
                () -> {
                    assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE);
                }
        );

        importByType(DataImportType.PROJECTS);
        executeWithAwait(60, () ->
                assertTableHasRecords(PROFILE_SERVICE_PROJECT_TABLE)
        );

        importByType(DataImportType.MENTORS);
        executeWithAwait(120, () -> {
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_PROGRAMMING_LANGUAGES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_MENTORS_SERVICES_TABLE);
                    assertTableHasRecords(MENTOR_SERVICE_SERVICES_TABLE);
                }
        );

        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        String description = "Бесплатно всем";
        String testName = "Артём";
        jdbcTemplate.update(
                UPDATE_DESCRIPTION_MENTOR_SQL,
                description, testName
        );

        importByType(DataImportType.MENTORS);
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

    private void importByType(DataImportType type) {
        long id = startImport(type);

        await().atMost(IMPORT_TIMEOUT_TWO_MINUTES).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> {
                    String status = getImportStatus(id);
                    if (status.equals("FAILED")) {
                        throw new IllegalStateException("Import failed for id: "+ id);
                    }
                    Assertions.assertThat(status).isEqualTo("SUCCESS");
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

    private void awaitUsersImported() {
        executeWithAwait(60, () -> {
            assertTableHasRecords(AUTH_SERVICE_USERS_TABLE);
            assertTableHasRecords(AUTH_SERVICE_USERS_ROLES_TABLE);
        });
    }

    private void assertAuthUserCreatedPublished() {
        executeWithAwait(60, () -> {
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofSeconds(1));

            assertThat(records.count()).isGreaterThan(0);
            assertThat(records.iterator().next().value())
                    .contains("telegram_user_id");
        });
    }
    private void awaitProfilesImported() {
        executeWithAwait(60, () ->
                assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE)
        );
    }
    private void awaitMentorsImported() {
        executeWithAwait(120, () ->
                assertTableHasRecords(MENTOR_SERVICE_MENTORS_TABLE)
        );
    }
    private void awaitGuaranteedReviewsImported() {
        executeWithAwait(60, () ->
                assertTableHasRecords(
                        MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE
                )
        );
    }

    private void assertAllMentorsHaveProfiles() {
        executeWithAwait(60, () -> {
            Integer mentorsWithoutProfiles = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM mentor_service.mentors m
                LEFT JOIN profile_service.profiles p
                    ON p.telegram_user_id = m.mentor_telegram_user_id
                WHERE p.id IS NULL
                """,
                    Integer.class
            );

            assertThat(mentorsWithoutProfiles).isZero();
        });
    }
}
