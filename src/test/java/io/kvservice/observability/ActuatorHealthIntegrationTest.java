package io.kvservice.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
        webEnvironment = WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.grpc.server.port=0",
                "management.server.port=0",
                "management.server.address=127.0.0.1",
                "kvservice.tarantool.connect-timeout=500ms",
                "kvservice.tarantool.request-timeout=500ms"
        }
)
class ActuatorHealthIntegrationTest {

    private static final String USERNAME = "kvservice";
    private static final String PASSWORD = "kvservice";
    private static final int TARANTOOL_PORT = 3301;
    private static final DockerImageName TARANTOOL_IMAGE = DockerImageName.parse("tarantool/tarantool:3.2.1");

    @Container
    static final GenericContainer<?> TARANTOOL = new GenericContainer<>(TARANTOOL_IMAGE)
            .withExposedPorts(TARANTOOL_PORT)
            .withEnv("TT_APP_NAME", "kvservice")
            .withEnv("TT_INSTANCE_NAME", "instance-001")
            .withEnv("TT_APP_PASSWORD", PASSWORD)
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("docker", "tarantool", "config.yaml").toAbsolutePath()),
                    "/opt/tarantool/kvservice/config.yaml"
            )
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("docker", "tarantool", "app.lua").toAbsolutePath()),
                    "/opt/tarantool/kvservice/app.lua"
            )
            .waitingFor(Wait.forLogMessage(".*ready to accept requests.*\\n", 1)
                    .withStartupTimeout(Duration.ofSeconds(30)));

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @LocalServerPort
    private int applicationPort;

    @Value("${local.management.port}")
    private int managementPort;

    @DynamicPropertySource
    static void tarantoolProperties(DynamicPropertyRegistry registry) {
        if (!TARANTOOL.isRunning()) {
            TARANTOOL.start();
        }
        registry.add("kvservice.tarantool.host", TARANTOOL::getHost);
        registry.add("kvservice.tarantool.port", () -> TARANTOOL.getMappedPort(TARANTOOL_PORT));
        registry.add("kvservice.tarantool.username", () -> USERNAME);
        registry.add("kvservice.tarantool.password", () -> PASSWORD);
    }

    @Test
    void splitManagementPortKeepsLivenessAndReadinessSemanticsOnBothSurfaces() throws Exception {
        HttpResponse<String> livenessBeforeFailure = get(this.managementPort, "/actuator/health/liveness");
        HttpResponse<String> readinessBeforeFailure = get(this.managementPort, "/actuator/health/readiness");
        HttpResponse<String> livezBeforeFailure = get(this.applicationPort, "/livez");
        HttpResponse<String> readyzBeforeFailure = get(this.applicationPort, "/readyz");

        assertThat(livenessBeforeFailure.statusCode()).isEqualTo(200);
        assertThat(livenessBeforeFailure.body()).contains("\"status\":\"UP\"");
        assertThat(readinessBeforeFailure.statusCode()).isEqualTo(200);
        assertThat(readinessBeforeFailure.body()).contains("\"status\":\"UP\"");
        assertThat(livezBeforeFailure.statusCode()).isEqualTo(200);
        assertThat(livezBeforeFailure.body()).contains("\"status\":\"UP\"");
        assertThat(readyzBeforeFailure.statusCode()).isEqualTo(200);
        assertThat(readyzBeforeFailure.body()).contains("\"status\":\"UP\"");

        TARANTOOL.stop();

        HttpResponse<String> readinessAfterFailure = get(this.managementPort, "/actuator/health/readiness");
        HttpResponse<String> livenessAfterFailure = get(this.managementPort, "/actuator/health/liveness");
        HttpResponse<String> readyzAfterFailure = get(this.applicationPort, "/readyz");
        HttpResponse<String> livezAfterFailure = get(this.applicationPort, "/livez");

        assertThat(readinessAfterFailure.statusCode()).isGreaterThanOrEqualTo(500);
        assertThat(readinessAfterFailure.body()).containsAnyOf("\"status\":\"DOWN\"", "\"status\":\"OUT_OF_SERVICE\"");
        assertThat(livenessAfterFailure.statusCode()).isEqualTo(200);
        assertThat(livenessAfterFailure.body()).contains("\"status\":\"UP\"");
        assertThat(readyzAfterFailure.statusCode()).isGreaterThanOrEqualTo(500);
        assertThat(readyzAfterFailure.body()).containsAnyOf("\"status\":\"DOWN\"", "\"status\":\"OUT_OF_SERVICE\"");
        assertThat(livezAfterFailure.statusCode()).isEqualTo(200);
        assertThat(livezAfterFailure.body()).contains("\"status\":\"UP\"");
    }

    private HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .GET()
                .build();
        return this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
