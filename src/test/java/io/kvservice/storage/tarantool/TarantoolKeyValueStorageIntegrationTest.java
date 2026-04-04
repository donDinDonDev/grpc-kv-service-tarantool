package io.kvservice.storage.tarantool;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.RangeBatchQuery;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import io.tarantool.client.box.TarantoolBoxClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
class TarantoolKeyValueStorageIntegrationTest {

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
    private KeyValueStoragePort storage;

    @Autowired
    private TarantoolSpaceInitializer initializer;

    @Autowired
    private TarantoolBoxClient client;

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
    void truncateSpace() throws Exception {
        this.client.eval("if box.space.KV ~= nil then box.space.KV:truncate() end return true")
                .get(5, TimeUnit.SECONDS);
    }

    @Test
    void supportsPutGetDeleteAndExactCountAgainstRealTarantool() {
        assertThat(this.storage.count()).isZero();

        this.storage.put("alpha", StoredValue.bytes(new byte[] { 1, 2, 3 }));
        assertThat(this.storage.count()).isEqualTo(1);

        Optional<StoredEntry> stored = this.storage.get("alpha");
        assertThat(stored).isPresent();
        assertThat(bytesOf(stored.orElseThrow().value())).containsExactly(1, 2, 3);

        this.storage.put("alpha", StoredValue.bytes(new byte[] { 9 }));
        assertThat(this.storage.count()).isEqualTo(1);
        assertThat(bytesOf(this.storage.get("alpha").orElseThrow().value())).containsExactly(9);

        this.storage.delete("alpha");
        this.storage.delete("alpha");

        assertThat(this.storage.get("alpha")).isEmpty();
        assertThat(this.storage.count()).isZero();
    }

    @Test
    void preservesMissingNullAndEmptyBytesAsDifferentStorageStates() {
        this.storage.put("null-key", StoredValue.nullValue());
        this.storage.put("empty-key", StoredValue.bytes(new byte[0]));

        StoredEntry nullEntry = this.storage.get("null-key").orElseThrow();
        StoredEntry emptyEntry = this.storage.get("empty-key").orElseThrow();

        assertThat(nullEntry.value().isNull()).isTrue();
        assertThat(bytesOf(emptyEntry.value())).isEmpty();
        assertThat(this.storage.get("missing-key")).isEmpty();
    }

    @Test
    void readsRangeInBatchesWithExclusiveUpperBoundAndResumeCursor() {
        this.storage.put("a", StoredValue.bytes(new byte[] { 1 }));
        this.storage.put("b", StoredValue.nullValue());
        this.storage.put("c", StoredValue.bytes(new byte[0]));
        this.storage.put("d", StoredValue.bytes(new byte[] { 4 }));

        List<StoredEntry> firstBatch = this.storage.getRangeBatch(new RangeBatchQuery("b", "d", null, 1));
        List<StoredEntry> secondBatch = this.storage.getRangeBatch(new RangeBatchQuery("b", "d", firstBatch.getFirst().key(), 1));
        List<StoredEntry> thirdBatch = this.storage.getRangeBatch(new RangeBatchQuery("b", "d", secondBatch.getFirst().key(), 1));

        assertThat(firstBatch).extracting(StoredEntry::key).containsExactly("b");
        assertThat(firstBatch.getFirst().value().isNull()).isTrue();
        assertThat(secondBatch).extracting(StoredEntry::key).containsExactly("c");
        assertThat(bytesOf(secondBatch.getFirst().value())).isEmpty();
        assertThat(thirdBatch).isEmpty();
    }

    @Test
    void readsRangeUsingUtf8LexicographicBounds() {
        String utf8LowerKey = key(0xE000);
        String utf8UpperKey = key(0x10000);

        this.storage.put(utf8LowerKey, StoredValue.bytes(new byte[] { 1 }));
        this.storage.put(utf8UpperKey, StoredValue.bytes(new byte[] { 2 }));

        List<StoredEntry> batch = this.storage.getRangeBatch(new RangeBatchQuery(utf8LowerKey, utf8UpperKey, null, 10));

        assertThat(batch).extracting(StoredEntry::key).containsExactly(utf8LowerKey);
        assertThat(bytesOf(batch.getFirst().value())).containsExactly(1);
    }

    @Test
    void initializerIsIdempotentAndKeepsSchemaUsable() throws Exception {
        this.initializer.initialize();
        this.initializer.initialize();

        String engine = stringScalar("return box.space.KV.engine");
        String indexType = stringScalar("return box.space.KV.index.primary.type");
        Boolean unique = booleanScalar("return box.space.KV.index.primary.unique");

        assertThat(engine).isEqualTo("vinyl");
        assertThat(indexType).isEqualToIgnoringCase("TREE");
        assertThat(unique).isTrue();

        this.storage.put("post-init", StoredValue.nullValue());
        assertThat(this.storage.get("post-init")).isPresent();
        assertThat(this.storage.count()).isEqualTo(1);
    }

    private byte[] bytesOf(StoredValue value) {
        return ((StoredValue.BytesValue) value).bytes();
    }

    private Boolean booleanScalar(String script) throws Exception {
        return this.client.eval(script, new TypeReference<List<Boolean>>() {
                })
                .get(5, TimeUnit.SECONDS)
                .get()
                .getFirst();
    }

    private String stringScalar(String script) throws Exception {
        return this.client.eval(script, new TypeReference<List<String>>() {
                })
                .get(5, TimeUnit.SECONDS)
                .get()
                .getFirst();
    }

    private static String key(int codePoint) {
        return new String(Character.toChars(codePoint));
    }

}
