package org.example.e2etests;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.*;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfig.class)
class AuthorizationTest extends E2eTestBase {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration KAFKA_POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final String AUTH_ENDPOINT = "/api/auth/by-telegram";
    private static final String PROFILE_ENDPOINT = "/api/profile/profile";
    private static final String KAFKA_TOPIC = "auth.user.created";
    private static final String EXPECTED_ROLE = "STUDENT";

    @Value("${TELEGRAM_INIT_DATA}")
    private String telegramInitData;

    @Value("${TELEGRAM_UPDATED_INIT_DATA}")
    private String updatedTelegramInitData;

    @Test
    void shouldRegisterUserWithTelegramUsername() {
        String accessToken = authenticateViaTelegram(telegramInitData);
        JwtClaims claims = parseJwtClaims(accessToken);
        Long telegramUserId = extractTelegramUserIdFrom(claims);

        assertThat(claims.roles()).containsExactly(EXPECTED_ROLE);

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
        JwtClaims firstClaims = parseJwtClaims(firstToken);
        Long telegramUserId = extractTelegramUserIdFrom(firstClaims);

        String secondToken = authenticateViaTelegram(telegramInitData);
        JwtClaims secondClaims = parseJwtClaims(secondToken);

        assertThat(secondClaims.roles()).containsExactly(EXPECTED_ROLE);
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
        JwtClaims firstClaims = parseJwtClaims(firstToken);
        Long telegramUserId = extractTelegramUserIdFrom(firstClaims);

        String updatedToken = authenticateViaTelegram(updatedTelegramInitData);
        JwtClaims updatedClaims = parseJwtClaims(updatedToken);

        assertThat(updatedClaims.roles()).containsExactly(EXPECTED_ROLE);
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

    private String authenticateViaTelegram(String telegramInitData) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        HttpEntity<String> request = new HttpEntity<>(telegramInitData, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                buildGatewayUrl(AUTH_ENDPOINT),
                request,
                String.class
        );

        assertThat(response.getHeaders().getFirst("X-Access-Token")).isNotNull();
        return response.getHeaders().getFirst("X-Access-Token");
    }

    private JwtClaims parseJwtClaims(String jwtToken) {
        String[] parts = jwtToken.split("\\.");
        String payloadJson = new String(Base64.getUrlDecoder().decode(parts[1]));
        JsonNode claimsNode = parseJson(payloadJson);
        return new JwtClaims(claimsNode);
    }

    private Long extractTelegramUserIdFrom(JwtClaims claims) {
        assertThat(claims.subject()).isNotBlank();
        return Long.parseLong(claims.subject());
    }

    private ResponseEntity<JsonNode> fetchUserProfile(String token, Long telegramUserId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        headers.set("X-Access-Token", token);
        headers.set("X-Telegram-User-Id", String.valueOf(telegramUserId));
        HttpEntity<Void> request = new HttpEntity<>(headers);

        return restTemplate.exchange(
                buildGatewayUrl(PROFILE_ENDPOINT),
                HttpMethod.GET,
                request,
                JsonNode.class
        );
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
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "test-" + System.currentTimeMillis());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, KAFKA_POLL_TIMEOUT);
            assertThat(records.count()).isGreaterThan(0);
        }
    }

    private JsonNode parseJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse JSON", e);
        }
    }

    private String buildGatewayUrl(String path) {
        return "http://localhost:" + gateway.getMappedPort(8080) + path;
    }

    private record JwtClaims(JsonNode node) {
        String subject() {
            return node.get("sub").asText();
        }

        List<String> roles() {
            var rolesNode = node.get("roles");
            return StreamSupport.stream(rolesNode.spliterator(), false)
                    .map(JsonNode::asText)
                    .toList();
        }
    }
}