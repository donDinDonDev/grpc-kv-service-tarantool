package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kvservice.api.v1.CountRequest;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.NullMarker;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import io.kvservice.api.v1.RangeItem;
import io.kvservice.api.v1.RangeRequest;
import io.tarantool.client.box.TarantoolBoxClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
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
@SpringBootTest(properties = "spring.grpc.server.port=0")
class RangeCountGrpcIntegrationTest {

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
  void rangeReturnsEmptyStreamForValidRangeWithoutRows() {
    Iterator<RangeItem> items =
        this.blockingStub.range(RangeRequest.newBuilder().setKeySince("a").setKeyTo("c").build());

    assertThat(items.hasNext()).isFalse();
  }

  @Test
  void rangeRejectsEqualOrDescendingBounds() {
    assertThatThrownBy(
            () ->
                this.blockingStub
                    .range(RangeRequest.newBuilder().setKeySince("b").setKeyTo("b").build())
                    .hasNext())
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void rangeStreamsHalfOpenAscendingResultsAndPreservesNulls() {
    put("a", NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("skip")).build());
    put("b", NullableBytes.newBuilder().setNullValue(NullMarker.getDefaultInstance()).build());
    put("c", NullableBytes.newBuilder().setData(ByteString.empty()).build());
    put("d", NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("stop")).build());

    List<RangeItem> items =
        toList(
            this.blockingStub.range(
                RangeRequest.newBuilder().setKeySince("b").setKeyTo("d").build()));

    assertThat(items).extracting(item -> item.getRecord().getKey()).containsExactly("b", "c");
    assertThat(items.getFirst().getRecord().getValue().getKindCase())
        .isEqualTo(NullableBytes.KindCase.NULL_VALUE);
    assertThat(items.get(1).getRecord().getValue().getKindCase())
        .isEqualTo(NullableBytes.KindCase.DATA);
    assertThat(items.get(1).getRecord().getValue().getData()).isEmpty();
    assertThat(
            this.blockingStub
                .get(GetRequest.newBuilder().setKey("d").build())
                .getRecord()
                .getValue()
                .getData()
                .toStringUtf8())
        .isEqualTo("stop");
  }

  @Test
  void rangeUsesUtf8LexicographicOrderingForValidationAndExclusiveUpperBound() {
    String utf8LowerKey = key(0xE000);
    String utf8UpperKey = key(0x10000);

    put(utf8LowerKey, NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("lower")).build());
    put(utf8UpperKey, NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("upper")).build());

    List<RangeItem> items =
        toList(
            this.blockingStub.range(
                RangeRequest.newBuilder()
                    .setKeySince(utf8LowerKey)
                    .setKeyTo(utf8UpperKey)
                    .build()));

    assertThat(items).extracting(item -> item.getRecord().getKey()).containsExactly(utf8LowerKey);

    assertThatThrownBy(
            () ->
                this.blockingStub
                    .range(
                        RangeRequest.newBuilder()
                            .setKeySince(utf8UpperKey)
                            .setKeyTo(utf8LowerKey)
                            .build())
                    .hasNext())
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void countReturnsExactNumberOfStoredEntries() {
    assertThat(this.blockingStub.count(CountRequest.getDefaultInstance()).getCount()).isZero();

    put("one", NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("1")).build());
    put("two", NullableBytes.newBuilder().setNullValue(NullMarker.getDefaultInstance()).build());
    put("one", NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("updated")).build());

    assertThat(this.blockingStub.count(CountRequest.getDefaultInstance()).getCount()).isEqualTo(2);
  }

  private void put(String key, NullableBytes value) {
    this.blockingStub.put(PutRequest.newBuilder().setKey(key).setValue(value).build());
  }

  private List<RangeItem> toList(Iterator<RangeItem> items) {
    List<RangeItem> result = new java.util.ArrayList<>();
    items.forEachRemaining(result::add);
    return List.copyOf(result);
  }

  private static String key(int codePoint) {
    return new String(Character.toChars(codePoint));
  }
}
