package io.kvservice.storage.tarantool;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.RangeBatchQuery;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import io.kvservice.observability.TarantoolObservability;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
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
    private static final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final String SHARED_KEY = "shared";
    private static final String RESET_CONTENTION_GATE_SCRIPT = """
            local gate = rawget(_G, 's07_contention_gate')
            if gate ~= nil and gate.trigger ~= nil and box.space.KV ~= nil then
                box.space.KV:before_replace(nil, gate.trigger)
            end
            rawset(_G, 's07_contention_gate', nil)
            return true
            """;
    private static final String INSTALL_CONTENTION_GATE_SCRIPT = """
            local fiber = require('fiber')
            local key = ...
            local gate = rawget(_G, 's07_contention_gate')
            if gate ~= nil and gate.trigger ~= nil and box.space.KV ~= nil then
                box.space.KV:before_replace(nil, gate.trigger)
            end

            gate = {
                key = key,
                entered = 0,
                block_limit = 2,
                entered_notifications = fiber.channel(2),
                release_tokens = fiber.channel(0)
            }

            gate.trigger = function(old, new)
                local tuple = new
                if tuple == nil then
                    tuple = old
                end
                if tuple == nil or tuple[1] ~= gate.key then
                    return new
                end

                gate.entered = gate.entered + 1
                if gate.entered <= gate.block_limit then
                    gate.entered_notifications:put(gate.entered)
                    gate.release_tokens:get()
                end
                return new
            end

            rawset(_G, 's07_contention_gate', gate)
            box.space.KV:before_replace(gate.trigger)
            return true
            """;
    private static final String WAIT_FOR_NEXT_CONTENTION_ENTRY_SCRIPT = """
            local gate = rawget(_G, 's07_contention_gate')
            return gate.entered_notifications:get()
            """;
    private static final String RELEASE_ONE_BLOCKED_OPERATION_SCRIPT = """
            local gate = rawget(_G, 's07_contention_gate')
            gate.release_tokens:put(true)
            return true
            """;

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
        executeBooleanScript(this.client, RESET_CONTENTION_GATE_SCRIPT);
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
    void concurrentWritesFollowLastWriteWinsForSingleKey() throws Exception {
        try (TarantoolBoxClient firstClient = newTestClient();
                TarantoolBoxClient secondClient = newTestClient();
                TarantoolBoxClient controlClient = newTestClient()) {
            KeyValueStoragePort firstStorage = new TarantoolKeyValueStorage(
                    firstClient,
                    TEST_REQUEST_TIMEOUT,
                    new TarantoolObservability(new SimpleMeterRegistry(), ObservationRegistry.create(), Tracer.NOOP)
            );
            KeyValueStoragePort secondStorage = new TarantoolKeyValueStorage(
                    secondClient,
                    TEST_REQUEST_TIMEOUT,
                    new TarantoolObservability(new SimpleMeterRegistry(), ObservationRegistry.create(), Tracer.NOOP)
            );
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                installContentionGate(controlClient, SHARED_KEY);

                Future<?> firstPut = executor.submit(() -> firstStorage.put(SHARED_KEY, StoredValue.bytes(new byte[] { 1 })));
                awaitContentionEntry(controlClient, 1L);
                assertThat(firstPut.isDone()).isFalse();

                Future<?> secondPut = executor.submit(() -> secondStorage.put(SHARED_KEY, StoredValue.bytes(new byte[] { 2 })));
                awaitContentionEntry(controlClient, 2L);
                assertThat(firstPut.isDone()).isFalse();
                assertThat(secondPut.isDone()).isFalse();

                releaseBlockedOperation(controlClient);
                releaseBlockedOperation(controlClient);
                firstPut.get(5, TimeUnit.SECONDS);
                secondPut.get(5, TimeUnit.SECONDS);
            }
            finally {
                executeBooleanScript(controlClient, RESET_CONTENTION_GATE_SCRIPT);
                shutdown(executor);
            }
        }

        assertThat(bytesOf(this.storage.get(SHARED_KEY).orElseThrow().value())).containsExactly(2);
        assertThat(this.storage.count()).isEqualTo(1);
    }

    @Test
    void concurrentDeletesRemainIdempotentForSingleKey() throws Exception {
        this.storage.put(SHARED_KEY, StoredValue.bytes(new byte[] { 7 }));

        try (TarantoolBoxClient firstClient = newTestClient();
                TarantoolBoxClient secondClient = newTestClient();
                TarantoolBoxClient controlClient = newTestClient()) {
            KeyValueStoragePort firstStorage = new TarantoolKeyValueStorage(
                    firstClient,
                    TEST_REQUEST_TIMEOUT,
                    new TarantoolObservability(new SimpleMeterRegistry(), ObservationRegistry.create(), Tracer.NOOP)
            );
            KeyValueStoragePort secondStorage = new TarantoolKeyValueStorage(
                    secondClient,
                    TEST_REQUEST_TIMEOUT,
                    new TarantoolObservability(new SimpleMeterRegistry(), ObservationRegistry.create(), Tracer.NOOP)
            );
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                installContentionGate(controlClient, SHARED_KEY);

                Future<?> firstDelete = executor.submit(() -> firstStorage.delete(SHARED_KEY));
                awaitContentionEntry(controlClient, 1L);
                assertThat(firstDelete.isDone()).isFalse();

                Future<?> secondDelete = executor.submit(() -> secondStorage.delete(SHARED_KEY));
                awaitContentionEntry(controlClient, 2L);
                assertThat(firstDelete.isDone()).isFalse();
                assertThat(secondDelete.isDone()).isFalse();

                releaseBlockedOperation(controlClient);
                releaseBlockedOperation(controlClient);
                firstDelete.get(5, TimeUnit.SECONDS);
                secondDelete.get(5, TimeUnit.SECONDS);
            }
            finally {
                executeBooleanScript(controlClient, RESET_CONTENTION_GATE_SCRIPT);
                shutdown(executor);
            }
        }

        assertThat(this.storage.get(SHARED_KEY)).isEmpty();
        assertThat(this.storage.count()).isZero();
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

    private TarantoolBoxClient newTestClient() throws Exception {
        return TarantoolFactory.box()
                .withHost(TARANTOOL.getHost())
                .withPort(TARANTOOL.getMappedPort(TARANTOOL_PORT))
                .withUser(USERNAME)
                .withPassword(PASSWORD)
                .withConnectTimeout(TEST_REQUEST_TIMEOUT.toMillis())
                .withReconnectAfter(100)
                .withFetchSchema(true)
                .build();
    }

    private void installContentionGate(TarantoolBoxClient tarantoolClient, String key) throws Exception {
        assertThat(booleanScalar(tarantoolClient, INSTALL_CONTENTION_GATE_SCRIPT, List.of(key))).isTrue();
    }

    private void awaitContentionEntry(TarantoolBoxClient tarantoolClient, long expectedEntry) throws Exception {
        assertThat(numberScalar(tarantoolClient, WAIT_FOR_NEXT_CONTENTION_ENTRY_SCRIPT).longValue()).isEqualTo(expectedEntry);
    }

    private void releaseBlockedOperation(TarantoolBoxClient tarantoolClient) throws Exception {
        executeBooleanScript(tarantoolClient, RELEASE_ONE_BLOCKED_OPERATION_SCRIPT);
    }

    private void executeBooleanScript(TarantoolBoxClient tarantoolClient, String script) throws Exception {
        assertThat(booleanScalar(tarantoolClient, script, List.of())).isTrue();
    }

    private Boolean booleanScalar(String script) throws Exception {
        return this.client.eval(script, new TypeReference<List<Boolean>>() {
                })
                .get(5, TimeUnit.SECONDS)
                .get()
                .getFirst();
    }

    private Boolean booleanScalar(TarantoolBoxClient tarantoolClient, String script, List<?> args) throws Exception {
        return tarantoolClient.eval(script, args, new TypeReference<List<Boolean>>() {
                })
                .get(5, TimeUnit.SECONDS)
                .get()
                .getFirst();
    }

    private Number numberScalar(TarantoolBoxClient tarantoolClient, String script) throws Exception {
        return tarantoolClient.eval(script, new TypeReference<List<Number>>() {
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

    private void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

}
