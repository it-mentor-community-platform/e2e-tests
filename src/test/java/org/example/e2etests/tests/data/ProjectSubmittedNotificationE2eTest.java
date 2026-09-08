package org.example.e2etests.tests.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.api.Assertions;
import org.example.e2etests.dto.*;
import org.example.e2etests.tests.base.E2eTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.util.HttpHeadersTestUtils.createHeaders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
public class ProjectSubmittedNotificationE2eTest extends E2eTestBase {

    private static final String GITHUB_REPOSITORY_URL = "https://github.com/zhukovsd/currency-exchange-testNotificationMentor";
    private static final String PROGRAMMING_LANGUAGE = "JAVA";
    private static final String ROADMAP_PROJECT = "CURRENCY-EXCHANGE";
    private static final String UPDATED_PROJECT = "CURRENCY_EXCHANGE";
    private static final int REVIEW_PRICE = 10;
    private static final String MENTOR_TELEGRAM_URL = "https://github.com/zhukovsd-mentor1";
    private static final long MENTOR_ID = 12745679L;

    private static final Set<String> TABLE_TO_TRUNCATE = Set.of(
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
        truncateTables(TABLE_TO_TRUNCATE);

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
    void shouldCreateNotificationTaskWhenSubmittingProject() throws Exception {
        createMentor();
        String accessToken = authenticateViaTelegram(telegramInitData);
        createProjectViaFrontend(accessToken);
        getBotTasks();
    }

    private String authenticateViaTelegram(String telegramInitData) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, String> jsonBody = Collections.singletonMap("initDataRaw", telegramInitData);
        HttpEntity<Map<String, String>> request = new HttpEntity<>(jsonBody, headers);
        ResponseEntity<String> response = testRestTemplate.postForEntity(
                AUTH_ENDPOINT,
                request,
                String.class
        );
        Assertions.assertThat(response.getHeaders().getFirst("X-Access-Token")).isNotNull();
        return response.getHeaders().getFirst("X-Access-Token");
    }

    private void getBotTasks() throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth(botUsername, botPassword);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<TasksResponseDto> response = testRestTemplate.exchange(
                BOT_TASKS_ENDPOINT_COUNT_10,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                TasksResponseDto.class
        );
        assertEquals(HttpStatus.OK, response.getStatusCode());
        TaskDto notificationTask = response.getBody().tasks().stream()
                .filter(taskDto -> NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC.equals(taskDto.taskType()))
                .findFirst()
                .orElseThrow();
        TaskPayloadDto payloadDto = notificationTask.payload();
        assertNotNull(payloadDto, "Task payload should not be null");
        assertThat(payloadDto.mentors()).isNotNull();
        assertThat(payloadDto.mentors().getFirst().mentorTelegramUserId()).isEqualTo(MENTOR_ID);
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
        await().atMost(30, SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    assertProjectPersistedInGoogleSheet();
                    assertTableHasOneRecord(PROFILE_SERVICE_PROJECT_TABLE, clause);
                    assertNotificationTaskPersistedInDatabase(NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC);
                });
    }

    private void assertTableHasOneRecord(String tableName, String clause) {
        int count = JdbcTestUtils.countRowsInTableWhere(jdbcTemplate, tableName, clause);
        assertThat(count).isOne();
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
        MentorImportMessageDto payloadDto = objectMapper.readValue(payload, MentorImportMessageDto.class);
        assertNotNull(payloadDto, "Payload should not be null");

        assertNotNull(payloadDto.mentors(), "Mentors list should not be null");
        assertThat(payloadDto.mentors().isEmpty()).isFalse();

        MentorDto firstMentor = payloadDto.mentors().getFirst();
        assertThat(firstMentor.mentorTelegramUserId()).isEqualTo(MENTOR_ID);
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