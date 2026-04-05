package io.kvservice.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.kvservice.api.v1.CountRequest;
import io.kvservice.api.v1.KvServiceGrpc;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.grpc.server.lifecycle.GrpcServerLifecycle;
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
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(
    properties = {
      "spring.grpc.server.port=0",
      "management.tracing.sampling.probability=1.0",
      "kvservice.tarantool.connect-timeout=500ms",
      "kvservice.tarantool.request-timeout=500ms"
    })
class GrpcTracingIntegrationTest {

  private static final String USERNAME = "kvservice";
  private static final String PASSWORD = "kvservice";
  private static final int TARANTOOL_PORT = 3301;
  private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final Metadata.Key<String> REQUEST_ID_HEADER =
      Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> TRACEPARENT_HEADER =
      Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);
  private static final DockerImageName TARANTOOL_IMAGE =
      DockerImageName.parse("tarantool/tarantool:3.2.1");

  @Container
  static final GenericContainer<?> TARANTOOL =
      new GenericContainer<>(TARANTOOL_IMAGE)
          .withExposedPorts(TARANTOOL_PORT)
          .withEnv("TT_APP_NAME", "kvservice")
          .withEnv("TT_INSTANCE_NAME", "instance-001")
          .withEnv("TT_APP_PASSWORD", PASSWORD)
          .withCopyFileToContainer(
              MountableFile.forHostPath(
                  Path.of("docker", "tarantool", "config.yaml").toAbsolutePath()),
              "/opt/tarantool/kvservice/config.yaml")
          .withCopyFileToContainer(
              MountableFile.forHostPath(Path.of("docker", "tarantool", "app.lua").toAbsolutePath()),
              "/opt/tarantool/kvservice/app.lua")
          .waitingFor(
              Wait.forLogMessage(".*ready to accept requests.*\\n", 1)
                  .withStartupTimeout(Duration.ofSeconds(30)));

  @Autowired private GrpcServerLifecycle grpcServerLifecycle;

  private ManagedChannel channel;

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

  @AfterEach
  void tearDown() throws Exception {
    if (this.channel != null) {
      this.channel.shutdownNow();
      this.channel.awaitTermination(5, TimeUnit.SECONDS);
      this.channel = null;
    }
  }

  @Test
  void propagatesIncomingTraceContextIntoGrpcAndTarantoolFailureLogs(CapturedOutput output) {
    this.channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", this.grpcServerLifecycle.getPort())
            .usePlaintext()
            .build();

    Metadata requestHeaders = new Metadata();
    requestHeaders.put(REQUEST_ID_HEADER, "req-trace-123");
    requestHeaders.put(TRACEPARENT_HEADER, "00-" + TRACE_ID + "-00f067aa0ba902b7-01");
    KvServiceGrpc.KvServiceBlockingStub blockingStub =
        KvServiceGrpc.newBlockingStub(
            ClientInterceptors.intercept(
                this.channel, MetadataUtils.newAttachHeadersInterceptor(requestHeaders)));

    TARANTOOL.stop();

    assertThatThrownBy(() -> blockingStub.count(CountRequest.getDefaultInstance()))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
        .isIn(Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED);

    String grpcFailure = findLogLine(output, "\"event\":\"gRPC request failed\"");
    String tarantoolFailure = findLogLine(output, "\"event\":\"Tarantool operation failed\"");

    assertThat(grpcFailure)
        .contains("\"request.id\":\"req-trace-123\"")
        .contains("\"traceId\":\"" + TRACE_ID + "\"")
        .contains("\"grpc.method\":\"Count\"");
    assertThat(tarantoolFailure)
        .contains("\"request.id\":\"req-trace-123\"")
        .contains("\"traceId\":\"" + TRACE_ID + "\"")
        .contains("\"tarantool.operation\":\"count\"");
    assertThat(extractField(grpcFailure, "spanId")).isNotBlank();
    assertThat(extractField(tarantoolFailure, "spanId"))
        .isNotBlank()
        .isNotEqualTo(extractField(grpcFailure, "spanId"));
  }

  private String findLogLine(CapturedOutput output, String token) {
    return (output.getOut() + output.getErr())
        .lines()
        .filter(line -> line.contains(token))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing log line with token: " + token));
  }

  private String extractField(String line, String fieldName) {
    Matcher matcher =
        Pattern.compile("\"" + Pattern.quote(fieldName) + "\":\"([^\"]+)\"").matcher(line);
    assertThat(matcher.find()).as("field %s in %s", fieldName, line).isTrue();
    return matcher.group(1);
  }
}
