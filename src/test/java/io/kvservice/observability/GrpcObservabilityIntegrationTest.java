package io.kvservice.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.kvservice.api.v1.CountRequest;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import io.kvservice.api.v1.RangeItem;
import io.kvservice.api.v1.RangeRequest;
import io.micrometer.core.instrument.MeterRegistry;
import io.tarantool.client.box.TarantoolBoxClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
@SpringBootTest(properties = "spring.grpc.server.port=0")
class GrpcObservabilityIntegrationTest {

  private static final String USERNAME = "kvservice";
  private static final String PASSWORD = "kvservice";
  private static final int TARANTOOL_PORT = 3301;
  private static final Metadata.Key<String> REQUEST_ID_HEADER =
      Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER);
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

  @Autowired private TarantoolBoxClient client;

  @Autowired private GrpcServerLifecycle grpcServerLifecycle;

  @Autowired private MeterRegistry meterRegistry;

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

  @BeforeEach
  void setUp() throws Exception {
    this.client
        .eval("if box.space.KV ~= nil then box.space.KV:truncate() end return true")
        .get(5, TimeUnit.SECONDS);
    this.channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", this.grpcServerLifecycle.getPort())
            .usePlaintext()
            .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (this.channel != null) {
      this.channel.shutdownNow();
      this.channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void propagatesRequestIdAndRegistersGrpcAndTarantoolMetrics(CapturedOutput output) {
    AtomicReference<Metadata> responseHeaders = new AtomicReference<>();
    AtomicReference<Metadata> responseTrailers = new AtomicReference<>();
    Metadata requestHeaders = new Metadata();
    requestHeaders.put(REQUEST_ID_HEADER, "req-123");
    KvServiceGrpc.KvServiceBlockingStub blockingStub =
        KvServiceGrpc.newBlockingStub(
            ClientInterceptors.intercept(
                this.channel,
                MetadataUtils.newAttachHeadersInterceptor(requestHeaders),
                MetadataUtils.newCaptureMetadataInterceptor(responseHeaders, responseTrailers)));

    blockingStub.put(
        PutRequest.newBuilder()
            .setKey("metric-key")
            .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("value")).build())
            .build());
    blockingStub.get(GetRequest.newBuilder().setKey("metric-key").build());
    List<RangeItem> items =
        toList(
            blockingStub.range(RangeRequest.newBuilder().setKeySince("a").setKeyTo("z").build()));
    assertThat(items).hasSize(1);
    blockingStub.count(CountRequest.getDefaultInstance());

    assertThatThrownBy(() -> blockingStub.get(GetRequest.newBuilder().setKey("missing").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
        .isEqualTo(Status.Code.NOT_FOUND);

    Metadata capturedHeaders = responseHeaders.get();
    Metadata capturedTrailers = responseTrailers.get();
    String propagatedRequestId =
        capturedHeaders != null ? capturedHeaders.get(REQUEST_ID_HEADER) : null;
    if (propagatedRequestId == null && capturedTrailers != null) {
      propagatedRequestId = capturedTrailers.get(REQUEST_ID_HEADER);
    }
    assertThat(propagatedRequestId).isEqualTo("req-123");
    assertThat(
            this.meterRegistry
                .get("kvservice.grpc.server.request.count")
                .tag("method", "Put")
                .counter()
                .count())
        .isGreaterThanOrEqualTo(1);
    assertThat(
            this.meterRegistry
                .get("kvservice.grpc.server.request.latency")
                .tag("method", "Count")
                .tag("status", "OK")
                .timer()
                .count())
        .isEqualTo(1);
    assertThat(
            this.meterRegistry
                .get("kvservice.grpc.server.error.count")
                .tag("method", "Get")
                .tag("status", "NOT_FOUND")
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(this.meterRegistry.find("kvservice.grpc.range.stream.active").gauge().value())
        .isZero();
    assertThat(
            this.meterRegistry
                .get("kvservice.grpc.range.stream.items")
                .tag("status", "OK")
                .summary()
                .totalAmount())
        .isEqualTo(1);
    assertThat(
            this.meterRegistry
                .get("kvservice.tarantool.operation.count")
                .tag("operation", "count")
                .tag("outcome", "success")
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(
            this.meterRegistry
                .get("kvservice.tarantool.operation.latency")
                .tag("operation", "range_batch")
                .tag("outcome", "success")
                .timer()
                .count())
        .isGreaterThanOrEqualTo(1);
    assertThat(output.getOut() + output.getErr())
        .contains("\"event\":\"gRPC request failed\"")
        .contains("\"request.id\":\"req-123\"")
        .contains("\"grpc.method\":\"Get\"")
        .contains("\"grpc.status\":\"NOT_FOUND\"")
        .contains("\"key.bytes\":\"7\"")
        .contains("\"key.sha256\":\"ffa63583dfa6706b\"");
  }

  private List<RangeItem> toList(Iterator<RangeItem> items) {
    List<RangeItem> result = new java.util.ArrayList<>();
    items.forEachRemaining(result::add);
    return List.copyOf(result);
  }
}
