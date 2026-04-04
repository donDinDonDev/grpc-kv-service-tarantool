package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kvservice.api.v1.DeleteRequest;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.NullMarker;
import io.kvservice.api.v1.PutRequest;
import io.tarantool.client.box.TarantoolBoxClient;
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
class UnaryCrudGrpcIntegrationTest {

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

    @Autowired
    private TarantoolBoxClient client;

    @Autowired
    private GrpcServerLifecycle grpcServerLifecycle;

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
        this.client.eval("if box.space.KV ~= nil then box.space.KV:truncate() end return true")
                .get(5, TimeUnit.SECONDS);
        this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", this.grpcServerLifecycle.getPort())
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
    void putRejectsMissingValueAsInvalidArgument() {
        assertThatThrownBy(() -> this.blockingStub.put(PutRequest.newBuilder().setKey("alpha").build()))
                .isInstanceOf(StatusRuntimeException.class)
                .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void getDistinguishesMissingNullEmptyBytesAndOverwrite() {
        assertThatThrownBy(() -> this.blockingStub.get(GetRequest.newBuilder().setKey("missing").build()))
                .isInstanceOf(StatusRuntimeException.class)
                .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
                .isEqualTo(Status.Code.NOT_FOUND);

        this.blockingStub.put(PutRequest.newBuilder()
                .setKey("null-key")
                .setValue(NullableBytes.newBuilder().setNullValue(NullMarker.getDefaultInstance()).build())
                .build());
        this.blockingStub.put(PutRequest.newBuilder()
                .setKey("empty-key")
                .setValue(NullableBytes.newBuilder().setData(com.google.protobuf.ByteString.empty()).build())
                .build());
        this.blockingStub.put(PutRequest.newBuilder()
                .setKey("alpha")
                .setValue(NullableBytes.newBuilder().setData(com.google.protobuf.ByteString.copyFromUtf8("first")).build())
                .build());
        this.blockingStub.put(PutRequest.newBuilder()
                .setKey("alpha")
                .setValue(NullableBytes.newBuilder().setData(com.google.protobuf.ByteString.copyFromUtf8("second")).build())
                .build());

        assertThat(this.blockingStub.get(GetRequest.newBuilder().setKey("null-key").build()).getRecord().getValue().getKindCase())
                .isEqualTo(NullableBytes.KindCase.NULL_VALUE);
        assertThat(this.blockingStub.get(GetRequest.newBuilder().setKey("empty-key").build()).getRecord().getValue().getKindCase())
                .isEqualTo(NullableBytes.KindCase.DATA);
        assertThat(this.blockingStub.get(GetRequest.newBuilder().setKey("empty-key").build()).getRecord().getValue().getData())
                .isEmpty();
        assertThat(this.blockingStub.get(GetRequest.newBuilder().setKey("alpha").build()).getRecord().getValue().getData().toStringUtf8())
                .isEqualTo("second");
    }

    @Test
    void deleteIsIdempotentAndRemovesExistingRecord() {
        this.blockingStub.put(PutRequest.newBuilder()
                .setKey("delete-me")
                .setValue(NullableBytes.newBuilder().setData(com.google.protobuf.ByteString.copyFrom(new byte[] { 1 })).build())
                .build());

        this.blockingStub.delete(DeleteRequest.newBuilder().setKey("delete-me").build());
        this.blockingStub.delete(DeleteRequest.newBuilder().setKey("delete-me").build());

        assertThatThrownBy(() -> this.blockingStub.get(GetRequest.newBuilder().setKey("delete-me").build()))
                .isInstanceOf(StatusRuntimeException.class)
                .extracting(failure -> ((StatusRuntimeException) failure).getStatus().getCode())
                .isEqualTo(Status.Code.NOT_FOUND);
    }
}
