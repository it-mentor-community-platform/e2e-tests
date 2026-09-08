package org.example.e2etests.tests.data;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.example.e2etests.dto.CreateProjectRequest;
import org.example.e2etests.tests.base.E2eTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.test.jdbc.JdbcTestUtils;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.example.e2etests.util.HttpHeadersTestUtils.createHeaders;

@Slf4j
public class ProjectBookkepingE2eImportTest extends E2eTestBase {

    private static final Set<String> TABLE_TO_TRUNCATE = Set.of(
            PROFILE_SERVICE_PROJECT_TABLE,
            PROJECT_SERVICE_PROJECTS_TABLE,
            BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE
    );

    @BeforeEach
    void setUp() {
        truncateTables(TABLE_TO_TRUNCATE);

        assertTableIsEmpty(PROFILE_SERVICE_PROJECT_TABLE);
        assertTableIsEmpty(PROJECT_SERVICE_PROJECTS_TABLE);
        assertTableIsEmpty(BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE);

        kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
        assertKafkaTopicEmpty("projects.project.created");
    }

    @AfterEach
    void clearTable() {
        truncateTables(TABLE_TO_TRUNCATE);
    }

    @Test
    void shouldCreateProjectAndSaveToDatabaseAndGoogleSheets() throws IOException, InterruptedException {
        CreateProjectRequest requestBody = CreateProjectRequest.builder()
                .githubRepositoryUrl("https://github.com/zhukovsd/currency-exchange-test")
                .programmingLanguage("Java")
                .roadmapProject("CURRENCY-EXCHANGE")
                .build();

        startImport("/api/project/project", requestBody);
        Thread.sleep(5000);
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertTableHasRecords("project_service.projects");
                    assertTableHasRecords("profile_service.project");
                });

        List<List<Object>> values = googleSheetsHelper.readSheet(testSpreadsheetId, "Projects!A:ZZ");
        boolean projectFound = values.stream()
                .anyMatch(row -> row.toString().contains("currency-exchange-test"));
        assertThat(projectFound).isTrue();
    }

    @Test
    void shouldCreateProjectAndSaveToDatabaseAndGoogleSheetsFromTgBot() throws IOException, InterruptedException {
        profileCreate();
        CreateProjectRequest requestBody = CreateProjectRequest.builder()
                .authorTelegramUserId(123456789L)
                .githubRepositoryUrl("https://github.com/zhukovsd/hangman-test")
                .programmingLanguage("Java")
                .roadmapProject("HANGMAN")
                .authorTelegramUsername("zhukovsd")
                .projectSourceType("TELEGRAM_BOT")
                .build();

        startImport("/api/project/internal/project", requestBody);
        Thread.sleep(5000);
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertTableHasRecords("project_service.projects");
                    assertTableHasRecords("profile_service.project");
                });

        List<List<Object>> values = googleSheetsHelper.readSheet(testSpreadsheetId, "Projects!A:ZZ");
        boolean projectFound = values.stream()
                .anyMatch(row -> row.toString().contains("hangman-test"));
        assertThat(projectFound).isTrue();
    }

    @Test
    void shouldCreateProjectAndSaveToDatabaseFromDataImporter() throws InterruptedException {
        CreateProjectRequest requestBody = CreateProjectRequest.builder()
                .authorTelegramUserId(123456789L)
                .githubRepositoryUrl("https://github.com/zhukovsd/hangman-test2")
                .programmingLanguage("Java")
                .roadmapProject("HANGMAN")
                .authorTelegramUsername("zhukovsd")
                .addedTimestamp("1765628000")
                .projectSourceType("DATA_IMPORTER")
                .build();

        startImport("/api/project/internal/project", requestBody);
        Thread.sleep(5000);
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertTableHasRecords("project_service.projects");
                    assertTableHasRecords("profile_service.project");
                });
    }

    @Test
    void shouldCreateTelegramBotTaskWhenProjectCreatedNotFromTelegramBot() {
        CreateProjectRequest requestBody = CreateProjectRequest.builder()
                .githubRepositoryUrl("https://github.com/zhukovsd/telegram-bot-task-test")
                .programmingLanguage("Java")
                .roadmapProject("CURRENCY-EXCHANGE")
                .build();

        startImport("/api/project/project", requestBody);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertTableHasRecords("telegram_bot_adapter.telegram_bot_tasks")
                );
    }

    @Test
    void shouldNotCreateTelegramBotTaskWhenProjectCreatedFromTelegramBot() {
        CreateProjectRequest requestBody = CreateProjectRequest.builder()
                .authorTelegramUserId(123456789L)
                .authorTelegramUsername("zhukovsd")
                .githubRepositoryUrl("https://github.com/zhukovsd/telegram-bot-task-test-from-tg-bot")
                .programmingLanguage("Java")
                .roadmapProject("CURRENCY-EXCHANGE")
                .projectSourceType("TELEGRAM_BOT")
                .build();

        startImport("/api/project/internal/project", requestBody);

        String clause = "task_type='%s'".formatted(PROJECTS_PROJECT_CREATED_TOPIC);
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertTableIsEmpty("telegram_bot_adapter.telegram_bot_tasks", clause)
                );
    }

    private void assertKafkaTopicEmpty(String topic) {
        kafkaConsumer.subscribe(List.of(topic));
        kafkaConsumer.poll(Duration.ofMillis(500));

        Set<TopicPartition> partitions = kafkaConsumer.assignment();
        kafkaConsumer.seekToEnd(partitions);

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
        assertThat(records.count()).isZero();
    }

    private void assertTableIsEmpty(String tableName, String clause) {
        int count = JdbcTestUtils.countRowsInTableWhere(jdbcTemplate, tableName, clause);
        assertThat(count).isZero();
    }

    private void startImport(String path, CreateProjectRequest requestBody) {
        int port = gateway.getMappedPort(8080);
        String host = gateway.getHost();
        if (path.contains("internal")) {
            port = projectService.getMappedPort(8080);
            host = projectService.getHost();
        }
        HttpEntity<CreateProjectRequest> requestEntity = new HttpEntity<>(requestBody, createHeaders(
                createAdminToken()));
        testRestTemplate.postForEntity(
                "http://" + host + ":" + port + path,
                requestEntity,
                String.class
        );
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

    private void profileCreate() {
        Long id = jdbcTemplate.queryForObject(
                """
                        INSERT INTO profile_service.profiles (telegram_user_id)
                        VALUES (?)
                        RETURNING id
                        """,
                Long.class,
                123456789L
        );
        jdbcTemplate.update(
                """
                        INSERT INTO profile_service.profiles_details
                            (profile_id, detail_name, detail_value)
                        VALUES (?, ?, ?)
                        """,
                id,
                "github_profile_url",
                "https://github.com/created_by_internal_request"
        );

        jdbcTemplate.update(
                """
                        INSERT INTO profile_service.profiles_details 
                            (profile_id, detail_name, detail_value) 
                        VALUES (?, ?, ?)
                        """,
                id,
                "telegram_url",
                "https://t.me/created_by_internal_request"
        );
    }
}
