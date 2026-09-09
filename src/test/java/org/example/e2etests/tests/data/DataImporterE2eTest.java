package org.example.e2etests.tests.data;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.awaitility.core.ThrowingRunnable;
import org.example.e2etests.tests.base.E2eTestBase;
import org.example.e2etests.util.JwtTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.util.HttpHeadersTestUtils.createHeaders;

@Slf4j
public class DataImporterE2eTest extends E2eTestBase {

    private static final String API_PROFILES_IMPORT = "/api/data-importer/start-profiles-import";
    private static final String API_PROJECTS_IMPORT = "/api/data-importer/start-projects-import";
    private static final String API_USERS_IMPORT = "/api/data-importer/start-users-import";
    private static final String API_MENTORS_IMPORT = "/api/data-importer/start-mentors-import";
    private static final String API_GUARANTEED_REVIEWS_IMPORT = "/api/data-importer/start-guaranteed-reviews-import";

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

        startImport(API_USERS_IMPORT);

        awaitUsersImported();
        assertAuthUserCreatedPublished();
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
    void shouldImportMentorsAndSendMessagesToKafka() {
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);

        kafkaConsumer.subscribe(List.of(NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC));

        startImport(API_PROFILES_IMPORT);
        executeWithAwait(60,
                () -> {
                    assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE);
                }
        );

        startImport(API_PROJECTS_IMPORT);
        executeWithAwait(60, () ->
                assertTableHasRecords(PROFILE_SERVICE_PROJECT_TABLE)
        );

        startImport(API_MENTORS_IMPORT);
        executeWithAwait(60, () -> {
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
    void shouldImportGuaranteedReviews() {
        assertTableIsEmpty(AUTH_SERVICE_USERS_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);

        kafkaConsumer.subscribe(List.of(AUTH_USER_CREATED_TOPIC));

        startImport(API_USERS_IMPORT);
        awaitUsersImported();
        assertAuthUserCreatedPublished();

        startImport(API_PROFILES_IMPORT);
        awaitProfilesImported();

        startImport(API_MENTORS_IMPORT);
        awaitMentorsImported();
        assertAllMentorsHaveProfiles();

        startImport(API_GUARANTEED_REVIEWS_IMPORT);
        awaitGuaranteedReviewsImported();
    }

    @Test
    void shouldNotOverwriteMentorDescription() {
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);

        startImport(API_PROFILES_IMPORT);
        executeWithAwait(60,
                () -> {
                    assertTableHasRecords(PROFILE_SERVICE_PROFILES_TABLE);
                }
        );

        startImport(API_PROJECTS_IMPORT);
        executeWithAwait(60, () ->
                assertTableHasRecords(PROFILE_SERVICE_PROJECT_TABLE)
        );

        startImport(API_MENTORS_IMPORT);
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

    private void startImport(String path) {
        testRestTemplate.postForEntity(
                path,
                new HttpEntity<>(createHeaders(JwtTestUtils.getAdminJWT(jwtSecret))),
                String.class
        );
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
