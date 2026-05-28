package org.example.e2etests.tests.base;

import io.jsonwebtoken.security.Keys;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.e2etests.config.GoogleSheetsClient;
import org.example.e2etests.config.TestcontainersConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import javax.crypto.SecretKey;
import java.util.Base64;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfig.class)
public abstract class E2eTestBase {
    @Value("${jwt.secret}")
    protected String jwtSecret;
    @Value("${telegram.init-data}")
    protected String telegramInitData;
    @Value("${telegram.updated-init-data}")
    protected String updatedTelegramInitData;
    @Value("${GOOGLE_TEST_SPREADSHEET_ID}")
    protected String testSpreadsheetId;
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

    protected SecretKey secretKey() {
        return Keys.hmacShaKeyFor(Base64.getDecoder().decode(jwtSecret));
    }
}
