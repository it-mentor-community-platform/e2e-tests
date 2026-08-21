package org.example.e2etests.tests.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.api.Assertions;
import org.example.e2etests.dto.CreateProjectRequest;
import org.example.e2etests.tests.base.E2eTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.HttpHeadersTestUtils.createHeaders;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ProjectSubmittedNotificationE2eTest extends E2eTestBase {

    private static final String GITHUB_REPOSITORY_URL = "https://github.com/zhukovsd/currency-exchange-testNotificationMentor";
    private static final String PROGRAMMING_LANGUAGE = "JAVA";
    private static final String ROADMAP_PROJECT = "CURRENCY-EXCHANGE";
    private static final String UPDATED_PROJECT = "CURRENCY_EXCHANGE";
    private static final int REVIEW_PRICE = 10;
    private static final String MENTOR_TELEGRAM_URL = "https://github.com/zhukovsd-mentor1";
    private static final long MENTOR_ID = 12745679L;

    private static final List<String> TABLES_TO_TRUNCATE = List.of(
            AUTH_SERVICE_USERS_TABLE,
            PROJECT_SERVICE_PROJECTS_TABLE,
            PROFILE_SERVICE_PROJECT_TABLE,
            PROFILE_SERVICE_PROFILES_TABLE,
            BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE,
            MENTOR_SERVICE_MENTORS_TABLE,
            MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE
    );

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute(
                """
                        TRUNCATE TABLE %s
                        RESTART IDENTITY CASCADE
                        """.formatted(String.join(", ", TABLES_TO_TRUNCATE))
        );

        assertTableIsEmpty(AUTH_SERVICE_USERS_TABLE);
        assertTableIsEmpty(PROJECT_SERVICE_PROJECTS_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROJECT_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_TABLE);
        assertTableIsEmpty(BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_MENTORS_TABLE);
        assertTableIsEmpty(MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE);
        assertKafkaTopicEmpty(List.of(
                PROJECTS_PROJECT_CREATED_TOPIC,
                NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC));
    }

    @Test
    void shouldCreateNotificationTaskWhenSubmittingProject() throws IOException {
        createMentor();
        String accessToken = authenticateViaTelegram(telegramInitData);
        createProjectViaFrontend(accessToken);
        getBotTasks();
    }

    private String authenticateViaTelegram(String telegramInitData) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        HttpEntity<String> request = new HttpEntity<>(telegramInitData, headers);

        ResponseEntity<String> response = testRestTemplate.postForEntity(
                AUTH_ENDPOINT,
                request,
                String.class
        );
        assertEquals(HttpStatus.OK, response.getStatusCode());

        String accessToken = response.getHeaders().getFirst("X-Access-Token");
        Assertions.assertThat(accessToken).isNotNull();

        Claims claims = parseJwt(accessToken);
        long telegramUserId = extractTelegramUserIdFrom(claims);

        String clause = "telegram_user_id='%s'".formatted(telegramUserId);
        assertTableHasOneRecord(AUTH_SERVICE_USERS_TABLE, clause);
        await().atMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertTableHasOneRecord(PROFILE_SERVICE_PROFILES_TABLE, clause)
                );
        return accessToken;
    }

    private void createProjectViaFrontend(String accessToken) {
        CreateProjectRequest requestBody = CreateProjectRequest.builder()
                .githubRepositoryUrl(GITHUB_REPOSITORY_URL)
                .programmingLanguage(PROGRAMMING_LANGUAGE)
                .roadmapProject(ROADMAP_PROJECT)
                .build();

        HttpEntity<CreateProjectRequest> requestEntity = new HttpEntity<>(requestBody, createHeaders(accessToken));
        ResponseEntity<String> response = testRestTemplate.postForEntity(
                PROJECT_FRONTEND_ENDPOINT,
                requestEntity,
                String.class
        );
        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        Claims claims = parseJwt(accessToken);
        long telegramUserId = extractTelegramUserIdFrom(claims);

        String clause = "author_telegram_user_id='%s'".formatted(telegramUserId);
        assertTableHasOneRecord(PROJECT_SERVICE_PROJECTS_TABLE, clause);
        await().atMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    assertProjectPersistedInGoogleSheet();
                    assertTableHasOneRecord(PROFILE_SERVICE_PROJECT_TABLE, clause);
                    assertNotificationTaskPersistedInDatabase(NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC);
                });
    }

    private void getBotTasks() throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth(botUsername, botPassword);

        ResponseEntity<String> response = testRestTemplate.exchange(
                BOT_TASKS_ENDPOINT_COUNT_10,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );
        assertEquals(HttpStatus.OK, response.getStatusCode());

        JsonNode tasks = objectMapper.readTree(response.getBody()).get("tasks");
        JsonNode notificationTask = StreamSupport.stream(tasks.spliterator(), false)
                .filter(task ->
                        NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC.equals(
                                task.path("taskType").asText()
                        )
                )
                .findFirst()
                .orElseThrow();

        JsonNode mentors = notificationTask.path("payload").path("mentors");
        assertThat(mentors.get(0)
                .path("mentor_telegram_user_id")
                .asLong()
        ).isEqualTo(MENTOR_ID);
    }

    private void assertTableHasOneRecord(String tableName, String clause) {
        int count = JdbcTestUtils.countRowsInTableWhere(jdbcTemplate, tableName, clause);
        assertThat(count).isOne();
    }

    private void assertTableIsEmpty(String tableName) {
        int count = JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName);
        assertThat(count).isZero();
    }

    private void assertNotificationTaskPersistedInDatabase(String taskType) throws JsonProcessingException {
        String payload = jdbcTemplate.queryForObject(
                """
                        SELECT payload::text
                        FROM telegram_bot_adapter.telegram_bot_tasks
                        WHERE task_type = ?
                        """,
                String.class,
                taskType
        );
        Assertions.assertThat(payload).isNotBlank();

        JsonNode mentors = objectMapper.readTree(payload).path("mentors");
        assertThat(
                mentors.get(0)
                        .path("mentor_telegram_user_id")
                        .asLong()
        ).isEqualTo(MENTOR_ID);
    }

    private void assertProjectPersistedInGoogleSheet() throws IOException {
        boolean projectFound = googleSheetsHelper.readSheet(testSpreadsheetId, "Projects!A:ZZ").stream()
                .anyMatch(row -> row.toString().contains(GITHUB_REPOSITORY_URL));
        assertThat(projectFound).isTrue();
    }

    private void assertKafkaTopicEmpty(List<String> topics) {
        kafkaConsumer.subscribe(topics);
        kafkaConsumer.poll(Duration.ofMillis(500));
        kafkaConsumer.seekToEnd(kafkaConsumer.assignment());

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
        assertThat(records.count()).isZero();
    }

    private Claims parseJwt(String token) {
        return Jwts.parser()
                .verifyWith(secretKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    private Long extractTelegramUserIdFrom(Claims claims) {
        Assertions.assertThat(claims.getSubject()).isNotBlank();
        return Long.parseLong(claims.getSubject());
    }

    private void createMentor() {
        Long mentorId = jdbcTemplate.queryForObject(
                """
                        INSERT INTO mentor_service.mentors (mentor_telegram_user_id, telegram_url, is_active)
                        VALUES (?, ?, ?)
                        RETURNING id
                        """,
                Long.class,
                MENTOR_ID,
                MENTOR_TELEGRAM_URL,
                true
        );

        jdbcTemplate.update(
                """
                        INSERT INTO mentor_service.guaranteed_reviews_prices
                            (mentor_id, project_type, language, price_usd)
                        VALUES (?, ?, ?, ?)
                        """,
                mentorId,
                UPDATED_PROJECT,
                PROGRAMMING_LANGUAGE,
                REVIEW_PRICE
        );
    }

}