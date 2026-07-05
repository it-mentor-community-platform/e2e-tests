package org.example.e2etests.tests.data;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import static org.example.e2etests.HttpHeadersTestUtils.createHeaders;

@Slf4j
public class DataImporterE2eTest extends E2eTestBase {

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE auth_service.users RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE profile_service.profiles RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE project_service.projects RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE mentor_service.guaranteed_reviews_prices RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE mentor_service.mentors RESTART IDENTITY CASCADE");
    }

    @Test
    void shouldImportUsersAndSendMessagesToKafka() throws InterruptedException {
        assertTableIsEmpty("auth_service.users");
        kafkaConsumer.subscribe(List.of("auth.user.created"));

        startImport("/api/data-importer/start-users-import");

        Thread.sleep(20_000);

        assertTableHasRecords("auth_service.users");

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);
        assertThat(records.iterator().next().value()).contains("telegram_user_id");
    }

    @Test
    void shouldImportProfilesAndProjects() throws InterruptedException {
        assertTableIsEmpty("profile_service.profiles");

        startImport("/api/data-importer/start-profiles-import");

        Thread.sleep(10_000);

        assertTableHasRecords("profile_service.profiles");

        assertTableIsEmpty("project_service.projects");
        kafkaConsumer.subscribe(List.of("projects.project.created"));
        startImport("/api/data-importer/start-projects-import");

        Thread.sleep(10_000);

        assertTableHasRecords("project_service.projects");

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);
        assertThat(records.iterator().next().value()).contains("author_telegram_user_id");
    }

    @Test
    void shouldImportMentorsAndReviewPricesAndSendMessagesToKafka() throws InterruptedException {
        assertTableIsEmpty("mentor_service.mentors");
        assertTableIsEmpty("mentor_service.guaranteed_reviews_prices");

        kafkaConsumer.subscribe(List.of("auth.user.created"));

        startImport("/api/data-importer/start-users-import");
        Thread.sleep(20_000);

        startImport("/api/data-importer/start-profiles-import");
        Thread.sleep(10_000);

        startImport("/api/data-importer/start-guaranteed-reviews-import");

        Thread.sleep(10_000);

        assertTableHasRecords("mentor_service.mentors");
        assertTableHasRecords("mentor_service.guaranteed_reviews_prices");

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);
        assertThat(records.iterator().next().value()).contains("telegram_user_id");
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
