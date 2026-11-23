package org.example.e2etests.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.Assertions;
import org.example.e2etests.dto.DataImportRequest;
import org.example.e2etests.dto.DataImportRequest.DataImportType;
import org.example.e2etests.tests.base.E2eTestBase;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;

import static org.awaitility.Awaitility.await;
import static org.example.e2etests.util.HttpHeadersTestUtils.createHeaders;

@Component
public class DataImportTestUtils extends E2eTestBase {

    private static final String API_IMPORT = "/api/data-importer/import";
    private static final String API_IMPORT_STATUS = "/api/data-importer/imports/";

    private static final Duration IMPORT_TIMEOUT_TWO_MINUTES = Duration.ofSeconds(120);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    public DataImportTestUtils() {}

    public void importByType(DataImportType type) {
        long id = startImport(type);

        await().atMost(IMPORT_TIMEOUT_TWO_MINUTES).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> {
                    String status = getImportStatus(id);
                    if (status.equals("FAILED")) {
                        throw new IllegalStateException("Import failed for id: "+ id);
                    }
                    Assertions.assertThat(status).isEqualTo("SUCCESS");
                });
    }

    private Long startImport(DataImportRequest.DataImportType type) {
        return Objects.requireNonNull(testRestTemplate.postForEntity(
                API_IMPORT,
                new HttpEntity<>(
                        new DataImportRequest(type),
                        createHeaders(JwtTestUtils.getAdminJWT(jwtSecret))
                ),
                JsonNode.class
        ).getBody()).path("id").asLong();
    }

    private String getImportStatus(Long id) {
        return Objects.requireNonNull(testRestTemplate.exchange(
                API_IMPORT_STATUS + id,
                HttpMethod.GET,
                new HttpEntity<>(
                        createHeaders(JwtTestUtils.getAdminJWT(jwtSecret))
                ),
                JsonNode.class
        ).getBody()).path("status").asText();
    }
}