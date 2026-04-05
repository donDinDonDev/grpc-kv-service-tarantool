package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.StoredValue;
import io.tarantool.client.box.TarantoolBoxClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
    properties = {
      "spring.grpc.server.port=0",
      "kvservice.grpc.max-request-bytes=128B",
      "kvservice.grpc.max-response-bytes=128B",
      "kvservice.limits.max-value-bytes=1KB"
    })
class GrpcPayloadLimitsIntegrationTest {

  private static final String USERNAME = "kvservice";
  private static final String PASSWORD = "kvservice";
  private static final int TARANTOOL_PORT = 3301;
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

  @Autowired private KeyValueStoragePort storage;

  @Autowired private GrpcServerLifecycle grpcServerLifecycle;

  private ManagedChannel channel;
  private KvServiceGrpc.KvServiceBlockingStub blockingStub;

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
    this.blockingStub = KvServiceGrpc.newBlockingStub(this.channel);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (this.channel != null) {
      this.channel.shutdownNow();
      this.channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void rejectsRequestsThatExceedConfiguredMaxRequestSize() {
    assertThatThrownBy(
            () ->
                this.blockingStub.put(
                    PutRequest.newBuilder()
                        .setKey("too-large-request")
                        .setValue(
                            NullableBytes.newBuilder()
                                .setData(ByteString.copyFrom(new byte[256]))
                                .build())
                        .build()))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void rejectsResponsesThatExceedConfiguredMaxResponseSize() {
    this.storage.put("too-large-response", StoredValue.bytes(new byte[256]));

    assertThatThrownBy(
            () ->
                this.blockingStub.get(GetRequest.newBuilder().setKey("too-large-response").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }
}
