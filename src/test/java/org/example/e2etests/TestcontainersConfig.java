package org.example.e2etests;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@TestConfiguration(proxyBeanMethods = false)
@SuppressWarnings("resource")
@EnableConfigurationProperties(DockerImageTagsProperties.class)
class TestcontainersConfig {

    private static final String GHCR = "ghcr.io/it-mentor-community-platform";

    private static final List<String> REQUIRED_TOPICS = List.of(
            "auth.user.created",
            "auth.user.authenticated",
            "projects.project.created"
    );

    @Value("${TESTCONTAINER_DOCKER_IMAGES_TAG}")
    private String defaultDockerImageTag;

    @Autowired
    private DockerImageTagsProperties tags;

    @Value("${TELEGRAM_BOT_TOKEN}")
    private String telegramBotToken;

    @Value("${HH_APP_ACCESS_TOKEN}")
    private String hhAppAccessToken;

    @Value("${HH_APP_EMAIL}")
    private String hhAppEmail;

    @Value("${GOOGLE_APPLICATION_CREDENTIALS_JSON}")
    private String googleCredentialsJson;

    @PostConstruct
    void init() {
        googleCredentialsJson = stripQuotes(googleCredentialsJson);
    }

    @Bean
    Network network() {
        return Network.newNetwork();
    }

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgres(Network network) {
        return new PostgreSQLContainer<>("postgres:18.0-alpine")
                .withNetwork(network)
                .withNetworkAliases("database")
                .withDatabaseName("it_mentor_community_platform")
                .withUsername("root")
                .withPassword("password")
                .withInitScript("init.sql");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka(Network network) throws Exception {
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener("kafka:19092");
        kafka.start();
        createTopics(kafka);
        return kafka;
    }

    private void createTopics(KafkaContainer kafka) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(
                    REQUIRED_TOPICS.stream()
                            .map(name -> new NewTopic(name, 1, (short) 1))
                            .toList()
            ).all().get(30, TimeUnit.SECONDS);
        }
    }

    @Bean
    GenericContainer<?> gateway(Network network) {
        return springService("gateway/gateway", resolveTag(tags.getGateway()), network);
    }

    @Bean
    GenericContainer<?> authService(Network network, PostgreSQLContainer<?> postgres) {
        return springService("auth-service/auth-service", resolveTag(tags.getAuthService()), network)
                .withNetworkAliases("auth-service")
                .withEnv("TELEGRAM_BOT_TOKEN", telegramBotToken)
                .withEnv("VALIDATE_TELEGRAM_INITDATA_TIMESTAMP", "false")
                .dependsOn(postgres);
    }

    @Bean
    GenericContainer<?> dataImporter(Network network, PostgreSQLContainer<?> postgres, KafkaContainer kafka) {
        return springService("data-importer/data-importer", resolveTag(tags.getDataImporter()), network)
                .withNetworkAliases("data-importer")
                .withEnv("GOOGLE_APPLICATION_CREDENTIALS_JSON", googleCredentialsJson)
                .dependsOn(postgres, kafka);
    }

    @Bean
    GenericContainer<?> profileService(Network network, PostgreSQLContainer<?> postgres, KafkaContainer kafka) {
        return springService("profile-service/profile-service", resolveTag(tags.getProfileService()), network)
                .withNetworkAliases("profile-service")
                .dependsOn(postgres, kafka);
    }

    @Bean
    GenericContainer<?> projectService(Network network, PostgreSQLContainer<?> postgres, KafkaContainer kafka) {
        return springService("project-service/project-service", resolveTag(tags.getProjectService()), network)
                .withNetworkAliases("project-service")
                .dependsOn(postgres, kafka);
    }

    @Bean
    GenericContainer<?> mentorService(Network network, PostgreSQLContainer<?> postgres, KafkaContainer kafka) {
        return springService("mentor-service/mentor-service", resolveTag(tags.getMentorService()), network)
                .withNetworkAliases("mentor-service")
                .dependsOn(postgres, kafka);
    }

    @Bean
    GenericContainer<?> jobMarketAnalytics(Network network, PostgreSQLContainer<?> postgres, KafkaContainer kafka) {
        return springService("job-market-analytics-service/job-market-analytics-service",
                resolveTag(tags.getJobMarketAnalyticsService()), network)
                .withNetworkAliases("job-market-analytics-service")
                .withEnv("HH_APP_ACCESS_TOKEN", hhAppAccessToken)
                .withEnv("HH_APP_EMAIL", hhAppEmail)
                .dependsOn(postgres, kafka);
    }

    private String resolveTag(String serviceTag) {
        return Optional.ofNullable(serviceTag)
                .filter(t -> !t.isBlank())
                .orElse(defaultDockerImageTag);
    }

    private GenericContainer<?> springService(String imagePath, String tag, Network network) {
        String serviceName = imagePath.substring(imagePath.lastIndexOf('/') + 1);
        return new GenericContainer<>(GHCR + "/" + imagePath + ":" + tag)
                .withNetwork(network)
                .withEnv("SPRING_PROFILES_ACTIVE", "local-stack")
                .withEnv("SPRING_KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
                .withExposedPorts(8080)
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(serviceName)))
                .waitingFor(
                        Wait.forHttp("/actuator/health")
                                .forPort(8080)
                                .forStatusCode(200)
                                .withStartupTimeout(Duration.ofMinutes(5))
                );
    }

    private String stripQuotes(String value) {
        if (value == null) {
            return null;
        }
        if ((value.startsWith("'") && value.endsWith("'")) ||
                (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
