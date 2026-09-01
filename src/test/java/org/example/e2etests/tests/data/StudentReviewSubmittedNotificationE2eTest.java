package org.example.e2etests.tests.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.api.Assertions;
import org.example.e2etests.dto.CreateProjectRequest;
import org.example.e2etests.dto.CreateReviewRequest;
import org.example.e2etests.tests.base.E2eTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.test.jdbc.JdbcTestUtils;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.HttpHeadersTestUtils.createHeaders;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class StudentReviewSubmittedNotificationE2eTest extends E2eTestBase {

    private static final long PROJECT_AUTHOR_TELEGRAM_USER_ID = 123456789L;
    private static final String PROJECT_AUTHOR_TELEGRAM_USERNAME = "student_e2e";
    private static final String GITHUB_REPOSITORY_URL =
            "https://github.com/zhukovsd/student-review-notification-test";
    private static final String REVIEW_URL =
            "https://github.com/zhukovsd/student-review-notification-test/pull/1";
    private static final String PROGRAMMING_LANGUAGE = "JAVA";
    private static final String ROADMAP_PROJECT = "CURRENCY-EXCHANGE";

    private static final List<String> TABLES_TO_TRUNCATE = List.of(
            AUTH_SERVICE_USERS_TABLE,
            PROJECT_SERVICE_PROJECTS_TABLE,
            PROFILE_SERVICE_PROJECT_TABLE,
            PROFILE_SERVICE_PROFILES_TABLE,
            BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE
    );

    @BeforeEach
    void setUp() {
        try { Thread.sleep(15_000); } catch (InterruptedException ignored) {}
        List<String> allTables = new java.util.ArrayList<>(TABLES_TO_TRUNCATE);
        if (!allTables.contains(PROJECT_SERVICE_REVIEWS_TABLE)) {
            allTables.add(PROJECT_SERVICE_REVIEWS_TABLE);
        }
        jdbcTemplate.execute(
                """
                        TRUNCATE TABLE %s
                        RESTART IDENTITY CASCADE
                        """.formatted(String.join(", ", allTables))
        );
        jdbcTemplate.execute("TRUNCATE TABLE " + BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE + " RESTART IDENTITY CASCADE");
        assertTableIsEmpty(AUTH_SERVICE_USERS_TABLE);
        assertTableIsEmpty(PROJECT_SERVICE_PROJECTS_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROJECT_TABLE);
        assertTableIsEmpty(PROFILE_SERVICE_PROFILES_TABLE);
        assertTableIsEmpty(BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE);

        assertKafkaTopicEmpty(List.of(
                PROJECTS_PROJECT_CREATED_TOPIC,
                NOTIFICATIONS_STUDENTS_REVIEW_SUBMITTED_TOPIC
        ));
    }

    @Test
    void shouldCreateTelegramBotTaskWhenStudentReviewSubmitted() throws IOException {
        createProjectAuthorProfile();
        createProjectViaInternalEndpoint();

        String accessToken = authenticateViaTelegram(telegramInitData);
        long reviewerTelegramUserId = extractTelegramUserIdFrom(parseJwt(accessToken));

        submitReviewViaFrontend(accessToken, reviewerTelegramUserId);

        await().atMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertNotificationTaskPersistedInDatabase(reviewerTelegramUserId)
                );

        assertNotificationTaskAvailableViaTelegramBotAdapterApi(reviewerTelegramUserId);
    }

    private String authenticateViaTelegram(String telegramInitData) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, String> jsonBody = java.util.Collections.singletonMap("initDataRaw", telegramInitData);
        HttpEntity<java.util.Map<String, String>> request = new HttpEntity<>(jsonBody, headers);
        ResponseEntity<String> response = testRestTemplate.postForEntity(
                AUTH_ENDPOINT,
                request,
                String.class
        );
        assertEquals(HttpStatus.OK, response.getStatusCode());
        String accessToken = response.getHeaders().getFirst("X-Access-Token");
        Assertions.assertThat(accessToken).isNotNull();
        long telegramUserId = extractTelegramUserIdFrom(parseJwt(accessToken));
        String clause = "telegram_user_id='%s'".formatted(telegramUserId);
        assertTableHasOneRecord(AUTH_SERVICE_USERS_TABLE, clause);
        await().atMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertTableHasOneRecord(PROFILE_SERVICE_PROFILES_TABLE, clause)
                );
        return accessToken;
    }

    private void createProjectAuthorProfile() {
        Long profileId = jdbcTemplate.queryForObject(
                """
                        INSERT INTO profile_service.profiles (telegram_user_id)
                        VALUES (?)
                        RETURNING id
                        """,
                Long.class,
                PROJECT_AUTHOR_TELEGRAM_USER_ID
        );

        jdbcTemplate.update(
                """
                        INSERT INTO profile_service.profiles_details
                            (profile_id, detail_name, detail_value)
                        VALUES (?, ?, ?)
                        """,
                profileId,
                "github_profile_url",
                "https://github.com/student-review-notification-author"
        );

        jdbcTemplate.update(
                """
                        INSERT INTO profile_service.profiles_details
                            (profile_id, detail_name, detail_value)
                        VALUES (?, ?, ?)
                        """,
                profileId,
                "telegram_url",
                "https://t.me/" + PROJECT_AUTHOR_TELEGRAM_USERNAME
        );
    }

    private void createProjectViaInternalEndpoint() {
        CreateProjectRequest requestBody = CreateProjectRequest.builder()
                .authorTelegramUserId(PROJECT_AUTHOR_TELEGRAM_USER_ID)
                .authorTelegramUsername(PROJECT_AUTHOR_TELEGRAM_USERNAME)
                .githubRepositoryUrl(GITHUB_REPOSITORY_URL)
                .programmingLanguage(PROGRAMMING_LANGUAGE)
                .roadmapProject(ROADMAP_PROJECT)
                .projectSourceType("TELEGRAM_BOT")
                .build();

        HttpEntity<CreateProjectRequest> requestEntity =
                new HttpEntity<>(requestBody, createHeaders(createAdminToken()));

        ResponseEntity<String> response = testRestTemplate.postForEntity(
                "http://" + projectService.getHost() + ":" + projectService.getMappedPort(8080)
                        + "/api/project/internal/project",
                requestEntity,
                String.class
        );

        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        String clause = "github_repository_url='%s'".formatted(GITHUB_REPOSITORY_URL);

        await().atMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertTableHasOneRecord(PROJECT_SERVICE_PROJECTS_TABLE, clause)
                );
    }

    private void submitReviewViaFrontend(String accessToken, long reviewerTelegramUserId) throws IOException {
        CreateReviewRequest requestBody = CreateReviewRequest.builder()
                .projectGithubRepositoryUrl(GITHUB_REPOSITORY_URL)
                .reviewUrl(REVIEW_URL)
                .build();
        HttpEntity<CreateReviewRequest> requestEntity =
                new HttpEntity<>(requestBody, createHeaders(accessToken));
        ResponseEntity<String> response = testRestTemplate.postForEntity(
                PROJECT_REVIEW_FRONTEND_ENDPOINT,
                requestEntity,
                String.class
        );
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        JsonNode responseBody = objectMapper.readTree(response.getBody());
        assertThat(responseBody.path("reviewer_telegram_user_id").asLong())
                .isEqualTo(reviewerTelegramUserId);
        assertThat(responseBody.path("url").asText())
                .isEqualTo(REVIEW_URL);
        assertThat(responseBody.path("project").path("github_repository_url").asText())
                .isEqualTo(GITHUB_REPOSITORY_URL);
    }

    private void assertNotificationTaskPersistedInDatabase(long reviewerTelegramUserId)
            throws JsonProcessingException {
        String payload = jdbcTemplate.queryForObject(
                """
                        SELECT payload::text
                        FROM telegram_bot_adapter.telegram_bot_tasks
                        WHERE task_type = ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                String.class,
                NOTIFICATIONS_STUDENTS_REVIEW_SUBMITTED_TOPIC
        );

        Assertions.assertThat(payload).isNotBlank();

        JsonNode payloadJson = objectMapper.readTree(payload);

        assertThat(payloadJson.path("reviewer_telegram_user_id").asLong())
                .isEqualTo(reviewerTelegramUserId);
        assertThat(payloadJson.path("url").asText())
                .isEqualTo(REVIEW_URL);
        assertThat(payloadJson.path("project").path("github_repository_url").asText())
                .isEqualTo(GITHUB_REPOSITORY_URL);
    }

    private void assertNotificationTaskAvailableViaTelegramBotAdapterApi(long reviewerTelegramUserId) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        String auth = "username:password";
        byte[] encodedAuth = java.util.Base64.getEncoder().encode(
                auth.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
        headers.set("Authorization", "Basic " + new String(encodedAuth));
        headers.setContentType(MediaType.APPLICATION_JSON);
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
                        NOTIFICATIONS_STUDENTS_REVIEW_SUBMITTED_TOPIC.equals(
                                task.path("taskType").asText()
                        )
                )
                .findFirst()
                .orElseThrow();
        long actualId = notificationTask.path("payload")
                .path("reviewer_telegram_user_id").asLong();
        assertThat(actualId).isEqualTo(reviewerTelegramUserId);
    }

    private void assertKafkaTopicEmpty(List<String> topics) {
        kafkaConsumer.subscribe(topics);
        kafkaConsumer.poll(Duration.ofMillis(500));
        kafkaConsumer.seekToEnd(kafkaConsumer.assignment());

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
        assertThat(records.count()).isZero();
    }

    private void assertTableHasOneRecord(String tableName, String clause) {
        int count = JdbcTestUtils.countRowsInTableWhere(jdbcTemplate, tableName, clause);
        assertThat(count).isOne();
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
}