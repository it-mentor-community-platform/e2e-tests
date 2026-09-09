package org.example.e2etests.tests.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.e2etests.config.GoogleSheetsClient;
import org.example.e2etests.config.TestcontainersConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.util.Collection;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfig.class)
public abstract class E2eTestBase {
    protected static final String AUTH_SERVICE_USERS_TABLE = "auth_service.users";
    protected static final String AUTH_SERVICE_USERS_ROLES_TABLE = "auth_service.roles";
    protected static final String PROJECT_SERVICE_PROJECTS_TABLE = "project_service.projects";
    protected static final String PROJECT_SERVICE_REVIEWS_TABLE = "project_service.reviews";
    protected static final String PROFILE_SERVICE_PROFILES_TABLE = "profile_service.profiles";
    protected static final String PROFILE_SERVICE_PROFILES_DETAILS_TABLE = "profile_service.profiles_details";
    protected static final String PROFILE_SERVICE_PROJECT_TABLE = "profile_service.project";
    protected static final String MENTOR_SERVICE_MENTORS_TABLE = "mentor_service.mentors";
    protected static final String MENTOR_SERVICE_GUARANTEED_REVIEWS_PRICES_TABLE = "mentor_service.guaranteed_reviews_prices";
    protected static final String MENTOR_SERVICE_MENTOR_DESCRIPTIONS_TABLE = "mentor_service.mentor_descriptions";
    protected static final String MENTOR_SERVICE_MENTORS_PROGRAMMING_LANGUAGES_TABLE = "mentor_service.mentors_programming_languages";
    protected static final String MENTOR_SERVICE_PROGRAMMING_LANGUAGES_TABLE = "mentor_service.programming_languages";
    protected static final String MENTOR_SERVICE_MENTORS_SERVICES_TABLE = "mentor_service.mentors_services";
    protected static final String MENTOR_SERVICE_SERVICES_TABLE = "mentor_service.services";

    protected static final String BOT_ADAPTER_TELEGRAM_BOT_TASKS_TABLE = "telegram_bot_adapter.telegram_bot_tasks";
    protected static final String AUTH_USER_CREATED_TOPIC = "auth.user.created";
    protected static final String PROJECTS_PROJECT_CREATED_TOPIC = "projects.project.created";
    protected static final String NOTIFICATIONS_MENTORS_PROJECT_SUBMITTED_TOPIC = "notifications.mentors.project.submitted";
    protected static final String AUTH_ENDPOINT = "/api/auth/by-telegram";
    protected static final String PROJECT_FRONTEND_ENDPOINT = "/api/project/project";
    protected static final String BOT_TASKS_ENDPOINT_COUNT_10 = "/api/telegram-bot-adapter/tasks?count=10";
    protected static final String PROJECT_REVIEW_FRONTEND_ENDPOINT = "/api/project/review";
    protected static final String NOTIFICATIONS_STUDENTS_REVIEW_SUBMITTED_TOPIC = "notifications.students.review.submitted";

    @Value("${jwt.secret}")
    protected String jwtSecret;
    @Value("${telegram.init-data}")
    protected String telegramInitData;
    @Value("${telegram.updated-init-data}")
    protected String updatedTelegramInitData;
    @Value("${GOOGLE_TEST_SPREADSHEET_ID}")
    protected String testSpreadsheetId;
    @Value("${telegram-bot-adapter.auth.username}")
    protected String botUsername;
    @Value("${telegram-bot-adapter.auth.password}")
    protected String botPassword;
    @Autowired
    protected GoogleSheetsClient googleSheetsHelper;
    @Autowired
    protected KafkaConsumer<String, String> kafkaConsumer;
    @Autowired
    protected JdbcTemplate jdbcTemplate;
    @Autowired
    protected PostgreSQLContainer<?> postgres;
    @Autowired
    protected KafkaContainer kafka;
    @Autowired
    protected GenericContainer<?> gateway;
    @Autowired
    protected GenericContainer<?> authService;
    @Autowired
    protected GenericContainer<?> dataImporter;
    @Autowired
    protected GenericContainer<?> profileService;
    @Autowired
    protected GenericContainer<?> projectService;
    @Autowired
    protected GenericContainer<?> mentorService;
    @Autowired
    protected GenericContainer<?> jobMarketAnalytics;
    @Autowired
    protected GenericContainer<?> telegramBotAdapter;
    @Autowired
    protected TestRestTemplate testRestTemplate;
    @Autowired
    protected ObjectMapper objectMapper;


    protected void truncateTables(Collection<String> tables) {
        if (tables == null || tables.isEmpty()) {
            return;
        }
        jdbcTemplate.execute(
                """
                        TRUNCATE TABLE %s
                        RESTART IDENTITY CASCADE
                        """.formatted(String.join(", ", tables)));
    }

    protected void assertTableIsEmpty(String tableName) {
        int count = JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName);
        assertThat(count).isZero();
    }

    protected void assertTableHasRecords(String tableName) {
        int count = JdbcTestUtils.countRowsInTable(jdbcTemplate, tableName);
        assertThat(count).isGreaterThan(0);
    }
}