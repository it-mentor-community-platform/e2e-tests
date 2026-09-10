package org.example.e2etests.tests.auth;

import com.fasterxml.jackson.databind.JsonNode;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.e2etests.tests.base.E2eTestBase;
import org.example.e2etests.util.JwtTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class AuthenticationAndAuthorizationE2eTest extends E2eTestBase {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration KAFKA_POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final String AUTH_ENDPOINT = "/api/auth/by-telegram";
    private static final String PROFILE_ENDPOINT = "/api/profile/profile";
    private static final String KAFKA_TOPIC = "auth.user.created";
    private static final String EXPECTED_ROLE = "STUDENT";

    private static final Set<String> TABLES_TO_TRUNCATE = Set.of(
            PROFILE_SERVICE_PROFILES_TABLE,
            AUTH_SERVICE_USERS_TABLE,
            MENTOR_SERVICE_MENTORS_TABLE
    );

    @AfterEach
    void cleanTestData() {
        truncateTables(TABLES_TO_TRUNCATE);
    }

    @Test
    void shouldRegisterUserWithTelegramUsername() {
        String accessToken = authenticateViaTelegram(telegramInitData);
        Claims claims = parseJwt(accessToken);
        Long telegramUserId = extractTelegramUserIdFrom(claims);

        assertUserRole(claims);
        Awaitility.await()
                .atMost(AWAIT_TIMEOUT)
                .pollInterval(AWAIT_POLL_INTERVAL)
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertThat(fetchUserProfile(accessToken, telegramUserId))
                                .isNotNull()
                                .extracting(ResponseEntity::getStatusCode)
                                .isEqualTo(HttpStatus.OK)
                );

        assertUserPersistedInDatabase(telegramUserId);
        assertProfilePersistedInDatabase(telegramUserId);
        assertKafkaEventPublished();
    }

    @Test
    void shouldAuthorizeExistingUser() {

        String firstToken = authenticateViaTelegram(telegramInitData);
        Claims firstClaims = parseJwt(firstToken);
        Long telegramUserId = extractTelegramUserIdFrom(firstClaims);

        String secondToken = authenticateViaTelegram(telegramInitData);
        Claims secondClaims = parseJwt(secondToken);

        assertUserRole(secondClaims);
        assertThat(extractTelegramUserIdFrom(secondClaims)).isEqualTo(telegramUserId);

        Awaitility.await().atMost(AWAIT_TIMEOUT).pollInterval(AWAIT_POLL_INTERVAL)
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertThat(fetchUserProfile(secondToken, telegramUserId))
                                .extracting(ResponseEntity::getStatusCode)
                                .isEqualTo(HttpStatus.OK)
                );

        List<Map<String, Object>> users = jdbcTemplate.queryForList(
                "SELECT * FROM auth_service.users WHERE telegram_user_id = ?", telegramUserId);
        assertThat(users).hasSize(1);
    }

    @Test
    void shouldAuthorizeUserWithUpdatedData() {
        String firstToken = authenticateViaTelegram(telegramInitData);
        Claims firstClaims = parseJwt(firstToken);
        Long telegramUserId = extractTelegramUserIdFrom(firstClaims);

        String updatedToken = authenticateViaTelegram(updatedTelegramInitData);
        Claims updatedClaims = parseJwt(updatedToken);

        assertUserRole(updatedClaims);
        assertThat(extractTelegramUserIdFrom(updatedClaims)).isEqualTo(telegramUserId);

        Awaitility.await()
                .atMost(AWAIT_TIMEOUT)
                .pollInterval(AWAIT_POLL_INTERVAL)
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertThat(fetchUserProfile(updatedToken, telegramUserId))
                                .extracting(ResponseEntity::getStatusCode)
                                .isEqualTo(HttpStatus.OK)
                );

        List<Map<String, Object>> users = jdbcTemplate.queryForList(
                "SELECT * FROM auth_service.users WHERE telegram_user_id = ?", telegramUserId);
        assertThat(users).hasSize(1);
    }

    @Test
    void shouldUpdateMentorTelegramUrlFromAuthenticatedUserTelegramUsername() {
        String firstToken = authenticateViaTelegram(telegramInitData);
        Claims firstClaims = parseJwt(firstToken);
        Long telegramUserId = extractTelegramUserIdFrom(firstClaims);

        upsertInternalUser(telegramUserId, List.of("MENTOR"));
        insertMentor(telegramUserId, "https://t.me/old_username");

        authenticateViaTelegram(updatedTelegramInitData);

        Awaitility.await()
                .atMost(AWAIT_TIMEOUT)
                .pollInterval(AWAIT_POLL_INTERVAL)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    Map<String, Object> mentor = jdbcTemplate.queryForMap(
                            "SELECT * FROM mentor_service.mentors WHERE mentor_telegram_user_id = ?",
                            telegramUserId
                    );

                    assertThat(mentor)
                            .containsEntry("mentor_telegram_user_id", telegramUserId)
                            .containsEntry("telegram_url", "https://t.me/yeahigh")
                            .containsEntry("is_active", true);
                });
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
        assertThat(response.getHeaders().getFirst("X-Access-Token")).isNotNull();
        return response.getHeaders().getFirst("X-Access-Token");
    }

    private Claims parseJwt(String token) {
        return Jwts.parser()
                .verifyWith(JwtTestUtils.secretKey(jwtSecret))
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    private Long extractTelegramUserIdFrom(Claims claims) {
        assertThat(claims.getSubject()).isNotBlank();
        return Long.parseLong(claims.getSubject());
    }

    private ResponseEntity<JsonNode> fetchUserProfile(String token, Long telegramUserId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        headers.set("X-Access-Token", token);
        headers.set("X-Telegram-User-Id", String.valueOf(telegramUserId));
        HttpEntity<Void> request = new HttpEntity<>(headers);

        return testRestTemplate.exchange(
                PROFILE_ENDPOINT,
                HttpMethod.GET,
                request,
                JsonNode.class
        );
    }

    private void assertUserRole(Claims claims) {
        List<String> roles = claims.get("roles", List.class);
        assertThat(roles).contains(EXPECTED_ROLE);
    }

    private void assertUserPersistedInDatabase(Long telegramUserId) {
        Map<String, Object> user = jdbcTemplate.queryForMap(
                "SELECT * FROM auth_service.users WHERE telegram_user_id = ?",
                telegramUserId
        );
        assertThat(user).containsEntry("telegram_user_id", telegramUserId);
    }

    private void assertProfilePersistedInDatabase(Long telegramUserId) {
        Map<String, Object> profile = jdbcTemplate.queryForMap(
                "SELECT * FROM profile_service.profiles WHERE telegram_user_id = ?",
                telegramUserId
        );
        assertThat(profile).containsEntry("telegram_user_id", telegramUserId);
    }

    private void assertKafkaEventPublished() {
        kafkaConsumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(kafkaConsumer, KAFKA_POLL_TIMEOUT);
        assertThat(records.count()).isGreaterThan(0);
    }

    private void upsertInternalUser(Long telegramUserId, List<String> roles) {
        int port = authService.getMappedPort(8080);
        String host = authService.getHost();

        Map<String, Object> requestBody = Map.of(
                "telegram_user_id", telegramUserId,
                "roles", roles
        );

        testRestTemplate.postForEntity(
                "http://" + host + ":" + port + "/api/auth/internal/user",
                new HttpEntity<>(requestBody),
                String.class
        );
    }

    private void insertMentor(Long telegramUserId, String telegramUrl) {
        jdbcTemplate.update(
                """
                        INSERT INTO mentor_service.mentors (mentor_telegram_user_id, telegram_url, is_active)
                        VALUES (?, ?, true)
                        """,
                telegramUserId,
                telegramUrl
        );
    }
}