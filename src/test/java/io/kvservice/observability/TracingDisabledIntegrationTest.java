package io.kvservice.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import io.micrometer.core.instrument.MeterRegistry;
import io.tarantool.client.box.TarantoolBoxClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
@SpringBootTest(
    properties = {"spring.grpc.server.port=0", "management.tracing.export.enabled=false"})
class TracingDisabledIntegrationTest {

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
      this.channel = null;
    }
  }

  @Test
  void grpcRuntimeStillWorksWhenTracingExportIsDisabled() {
    AtomicReference<Metadata> responseHeaders = new AtomicReference<>();
    Metadata requestHeaders = new Metadata();
    requestHeaders.put(REQUEST_ID_HEADER, "req-no-trace");
    KvServiceGrpc.KvServiceBlockingStub blockingStub =
        KvServiceGrpc.newBlockingStub(
            ClientInterceptors.intercept(
                this.channel,
                MetadataUtils.newAttachHeadersInterceptor(requestHeaders),
                MetadataUtils.newCaptureMetadataInterceptor(
                    responseHeaders, new AtomicReference<>())));

    blockingStub.put(
        PutRequest.newBuilder()
            .setKey("tracing-off-key")
            .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("value")).build())
            .build());
    assertThat(
            blockingStub
                .get(GetRequest.newBuilder().setKey("tracing-off-key").build())
                .getRecord()
                .getValue()
                .getData())
        .isEqualTo(ByteString.copyFromUtf8("value"));

    assertThat(responseHeaders.get().get(REQUEST_ID_HEADER)).isEqualTo("req-no-trace");
    assertThat(
            this.meterRegistry
                .get("kvservice.grpc.server.request.count")
                .tag("method", "Put")
                .counter()
                .count())
        .isGreaterThanOrEqualTo(1);
    assertThat(
            this.meterRegistry
                .get("kvservice.tarantool.operation.count")
                .tag("operation", "get")
                .tag("outcome", "success")
                .counter()
                .count())
        .isEqualTo(1);
  }
}
