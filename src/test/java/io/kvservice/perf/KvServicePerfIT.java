package io.kvservice.perf;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kvservice.KvServiceApplication;
import io.kvservice.api.v1.CountRequest;
import io.kvservice.api.v1.DeleteRequest;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import io.kvservice.api.v1.RangeItem;
import io.kvservice.api.v1.RangeRequest;
import io.tarantool.client.BaseOptions;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.grpc.server.lifecycle.GrpcServerLifecycle;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class KvServicePerfIT {

    private static final String USERNAME = "kvservice";
    private static final String PASSWORD = "kvservice";
    private static final int TARANTOOL_PORT = 3301;
    private static final DockerImageName TARANTOOL_IMAGE = DockerImageName.parse("tarantool/tarantool:3.2.1");
    private static final String REPORT_DIR_PROPERTY = "kvservice.perf.report-dir";
    private static final String PERF_COMMAND_PROPERTY = "kvservice.perf.command";
    private static final String SPACE_NAME = "KV";
    private static final int BULK_DATASET_SIZE = 4_000;
    private static final int BULK_WARMUP_SIZE = 256;
    private static final int BULK_CONCURRENCY = 8;
    private static final int UNARY_CONCURRENCY = 16;
    private static final int UNARY_ITERATIONS_PER_WORKER = 250;
    private static final int RANGE_DATASET_SIZE = 12_000;
    private static final int RANGE_MEASURED_RUNS = 5;
    private static final int COUNT_MEASURED_RUNS = 10;
    private static final int SATURATION_DATASET_SIZE = 2_048;
    private static final int SATURATION_REQUESTED_STREAMS = 24;
    private static final int SATURATION_DELAY_EVERY_ITEMS = 128;
    private static final long SATURATION_DELAY_NANOS = Duration.ofMillis(1).toNanos();
    private static final int VALUE_BYTES = 256;
    private static final String CANONICAL_MEASUREMENT_MODE =
            "management.tracing.export.enabled=false; внешний OTLP backend не используется; metrics/logging остаются включены";

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

    @Test
    void generatesReproduciblePerformanceEvidence() throws Exception {
        List<ScenarioResult> results = new ArrayList<>();

        ScenarioResult bulkPutResult;
        ScenarioResult concurrentUnaryResult;
        ScenarioResult longRangeDefaultResult;
        ScenarioResult exactCountDefaultResult;
        ScenarioResult saturationDefaultResult;
        ScenarioResult rangeBatch512Result;
        ScenarioResult rangeBatch1024Result;
        ScenarioResult saturationMaxStreams8Result;
        ScenarioResult saturationMaxStreams32Result;

        try (ServiceRuntime runtime = ServiceRuntime.start("default-runtime", Map.of())) {
            runtime.truncateSpace();
            warmUpBulkLifecycle(runtime);
            runtime.truncateSpace();

            bulkPutResult = measureBulkStage(runtime, BulkStage.PUT, BULK_DATASET_SIZE, BULK_CONCURRENCY, "bulk-put/defaults");
            results.add(bulkPutResult);
            results.add(measureBulkStage(runtime, BulkStage.GET, BULK_DATASET_SIZE, BULK_CONCURRENCY, "bulk-get/defaults"));
            results.add(measureBulkStage(runtime, BulkStage.DELETE, BULK_DATASET_SIZE, BULK_CONCURRENCY, "bulk-delete/defaults"));
            assertThat(runtime.blockingStub().count(CountRequest.getDefaultInstance()).getCount()).isZero();

            runtime.truncateSpace();
            warmUpConcurrentUnary(runtime);
            runtime.truncateSpace();
            concurrentUnaryResult = measureConcurrentUnaryCrud(runtime);
            results.add(concurrentUnaryResult);

            runtime.truncateSpace();
            runtime.seedRangeDatasetDirect("range-default", RANGE_DATASET_SIZE, VALUE_BYTES);
            warmUpRangeAndCount(runtime, "range-default", RANGE_DATASET_SIZE);
            longRangeDefaultResult = measureLongRange(runtime, "range-default", RANGE_DATASET_SIZE,
                    "long-range/default-batch-256");
            exactCountDefaultResult = measureExactCount(runtime, RANGE_DATASET_SIZE, "count/default-deadlines");
            results.add(longRangeDefaultResult);
            results.add(exactCountDefaultResult);

            runtime.truncateSpace();
            runtime.seedRangeDatasetDirect("stream-limit-default", SATURATION_DATASET_SIZE, VALUE_BYTES);
            saturationDefaultResult = measureConcurrentRangeSaturation(runtime, "stream-limit-default", SATURATION_DATASET_SIZE,
                    "range-stream-saturation/default-max-streams-16");
            results.add(saturationDefaultResult);
        }

        try (ServiceRuntime runtime = ServiceRuntime.start("range-batch-512", Map.of("kvservice.range.batch-size", "512"))) {
            runtime.truncateSpace();
            runtime.seedRangeDatasetDirect("range-b512", RANGE_DATASET_SIZE, VALUE_BYTES);
            warmUpRangeAndCount(runtime, "range-b512", RANGE_DATASET_SIZE);
            rangeBatch512Result = measureLongRange(runtime, "range-b512", RANGE_DATASET_SIZE, "long-range/batch-512");
            results.add(rangeBatch512Result);
        }

        try (ServiceRuntime runtime = ServiceRuntime.start("range-batch-1024", Map.of("kvservice.range.batch-size", "1024"))) {
            runtime.truncateSpace();
            runtime.seedRangeDatasetDirect("range-b1024", RANGE_DATASET_SIZE, VALUE_BYTES);
            warmUpRangeAndCount(runtime, "range-b1024", RANGE_DATASET_SIZE);
            rangeBatch1024Result = measureLongRange(runtime, "range-b1024", RANGE_DATASET_SIZE, "long-range/batch-1024");
            results.add(rangeBatch1024Result);
        }

        try (ServiceRuntime runtime = ServiceRuntime.start("max-streams-8", Map.of("kvservice.range.max-active-streams", "8"))) {
            runtime.truncateSpace();
            runtime.seedRangeDatasetDirect("stream-limit-8", SATURATION_DATASET_SIZE, VALUE_BYTES);
            saturationMaxStreams8Result = measureConcurrentRangeSaturation(runtime, "stream-limit-8", SATURATION_DATASET_SIZE,
                    "range-stream-saturation/max-streams-8");
            results.add(saturationMaxStreams8Result);
        }

        try (ServiceRuntime runtime = ServiceRuntime.start("max-streams-32", Map.of("kvservice.range.max-active-streams", "32"))) {
            runtime.truncateSpace();
            runtime.seedRangeDatasetDirect("stream-limit-32", SATURATION_DATASET_SIZE, VALUE_BYTES);
            saturationMaxStreams32Result = measureConcurrentRangeSaturation(runtime, "stream-limit-32", SATURATION_DATASET_SIZE,
                    "range-stream-saturation/max-streams-32");
            results.add(saturationMaxStreams32Result);
        }

        List<String> defaultsReview = List.of(
                "range.batch-size: measured on long-range scenario with 256, 512 and 1024 entries per internal batch in one canonical mode",
                "range.max-active-streams: measured on concurrent range saturation with 8, 16 and 32 permits in one canonical mode",
                "deadline/request-timeout review: compared against observed unary/range/count latencies under default 3s/15s/5s guardrails"
        );

        PerformanceReport report = new PerformanceReport(
                OffsetDateTime.now(),
                System.getProperty(PERF_COMMAND_PROPERTY, "./mvnw -Pperf verify"),
                CANONICAL_MEASUREMENT_MODE,
                EnvironmentSnapshot.capture(),
                results,
                defaultsReview,
                List.of(
                        "Все измерения выполнены на одном хосте: client, Spring Boot runtime и Tarantool делят CPU/IO и не образуют SLA-обещание.",
                        "Perf suite не содержит pass/fail порогов по throughput; он фиксирует evidence и не является обязательным PR gate.",
                        "Сценарии tuning сравнивают только один изменённый runtime knob за запуск и не смешивают tracing/export режимы."
                )
        );

        Path reportPath = writeReport(report);

        assertThat(reportPath).exists();
        assertThat(results).extracting(ScenarioResult::scenario).contains(
                "bulk-put/defaults",
                "bulk-get/defaults",
                "bulk-delete/defaults",
                "concurrent-unary-crud/defaults",
                "long-range/default-batch-256",
                "count/default-deadlines",
                "range-stream-saturation/default-max-streams-16",
                "long-range/batch-512",
                "long-range/batch-1024",
                "range-stream-saturation/max-streams-8",
                "range-stream-saturation/max-streams-32"
        );
        assertThat(bulkPutResult.throughputPerSecond()).isPositive();
        assertThat(concurrentUnaryResult.throughputPerSecond()).isPositive();
        assertThat(longRangeDefaultResult.throughputPerSecond()).isPositive();
        assertThat(exactCountDefaultResult.p95Millis()).isPositive();
        assertThat(saturationDefaultResult.notes()).contains("requested=" + SATURATION_REQUESTED_STREAMS);
        assertThat(rangeBatch512Result.totalUnits()).isEqualTo((long) RANGE_DATASET_SIZE * RANGE_MEASURED_RUNS);
        assertThat(rangeBatch1024Result.totalUnits()).isEqualTo((long) RANGE_DATASET_SIZE * RANGE_MEASURED_RUNS);
        assertThat(saturationMaxStreams8Result.notes()).contains("reject_status=UNAVAILABLE");
        assertThat(saturationMaxStreams32Result.notes()).contains("requested=" + SATURATION_REQUESTED_STREAMS);
    }

    private void warmUpBulkLifecycle(ServiceRuntime runtime) throws Exception {
        measureBulkStage(runtime, BulkStage.PUT, BULK_WARMUP_SIZE, 4, "warmup-bulk-put");
        measureBulkStage(runtime, BulkStage.GET, BULK_WARMUP_SIZE, 4, "warmup-bulk-get");
        measureBulkStage(runtime, BulkStage.DELETE, BULK_WARMUP_SIZE, 4, "warmup-bulk-delete");
    }

    private void warmUpConcurrentUnary(ServiceRuntime runtime) throws Exception {
        runtime.runConcurrent("warmup-unary", 4, workerIndex -> () -> {
            ManagedChannel channel = runtime.newChannel();
            try {
                KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel);
                byte[] payload = payload(workerIndex, 64);
                for (int iteration = 0; iteration < 25; iteration++) {
                    String key = unaryKey("warmup-unary", workerIndex, iteration);
                    stub.put(putRequest(key, payload));
                    assertThat(stub.get(GetRequest.newBuilder().setKey(key).build()).getRecord().getValue().getData().toByteArray())
                            .isEqualTo(payload);
                    stub.delete(DeleteRequest.newBuilder().setKey(key).build());
                }
            }
            finally {
                shutdownChannel(channel);
            }
            return List.<Long>of();
        });
    }

    private void warmUpRangeAndCount(ServiceRuntime runtime, String prefix, int datasetSize) {
        consumeRange(runtime.blockingStub(), rangeRequest(prefix, datasetSize));
        assertThat(runtime.blockingStub().count(CountRequest.getDefaultInstance()).getCount()).isEqualTo(datasetSize);
    }

    private ScenarioResult measureBulkStage(
            ServiceRuntime runtime,
            BulkStage stage,
            int datasetSize,
            int concurrency,
            String scenarioName
    ) throws Exception {
        long startedAt = System.nanoTime();
        List<List<Long>> workerSamples = runtime.runConcurrent(stage.name().toLowerCase(Locale.ROOT), concurrency, workerIndex -> () -> {
            List<Long> samples = new ArrayList<>();
            ManagedChannel channel = runtime.newChannel();
            try {
                KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel);
                for (int index = workerIndex; index < datasetSize; index += concurrency) {
                    String key = bulkKey(index);
                    byte[] payload = payload(index, VALUE_BYTES);
                    long operationStarted = System.nanoTime();
                    switch (stage) {
                        case PUT -> stub.put(putRequest(key, payload));
                        case GET -> assertThat(stub.get(GetRequest.newBuilder().setKey(key).build()).getRecord().getValue().getData()
                                .toByteArray()).isEqualTo(payload);
                        case DELETE -> stub.delete(DeleteRequest.newBuilder().setKey(key).build());
                    }
                    samples.add(System.nanoTime() - operationStarted);
                }
            }
            finally {
                shutdownChannel(channel);
            }
            return samples;
        });
        long wallClockNanos = System.nanoTime() - startedAt;
        List<Long> latencies = flatten(workerSamples);
        LatencySummary summary = LatencySummary.fromNanos(latencies);
        return new ScenarioResult(
                scenarioName,
                runtime.label(),
                "rpc",
                datasetSize,
                summary.sampleCount(),
                summary.p50Millis(),
                summary.p95Millis(),
                summary.p99Millis(),
                throughput(datasetSize, wallClockNanos),
                "concurrency=%d".formatted(concurrency)
        );
    }

    private ScenarioResult measureConcurrentUnaryCrud(ServiceRuntime runtime) throws Exception {
        long totalOperations = (long) UNARY_CONCURRENCY * UNARY_ITERATIONS_PER_WORKER * 3;
        long startedAt = System.nanoTime();
        List<List<Long>> workerSamples = runtime.runConcurrent("concurrent-unary", UNARY_CONCURRENCY, workerIndex -> () -> {
            List<Long> samples = new ArrayList<>(UNARY_ITERATIONS_PER_WORKER * 3);
            ManagedChannel channel = runtime.newChannel();
            try {
                KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel);
                for (int iteration = 0; iteration < UNARY_ITERATIONS_PER_WORKER; iteration++) {
                    String key = unaryKey("concurrent", workerIndex, iteration);
                    byte[] payload = payload(workerIndex * UNARY_ITERATIONS_PER_WORKER + iteration, VALUE_BYTES);

                    long putStarted = System.nanoTime();
                    stub.put(putRequest(key, payload));
                    samples.add(System.nanoTime() - putStarted);

                    long getStarted = System.nanoTime();
                    assertThat(stub.get(GetRequest.newBuilder().setKey(key).build()).getRecord().getValue().getData().toByteArray())
                            .isEqualTo(payload);
                    samples.add(System.nanoTime() - getStarted);

                    long deleteStarted = System.nanoTime();
                    stub.delete(DeleteRequest.newBuilder().setKey(key).build());
                    samples.add(System.nanoTime() - deleteStarted);
                }
            }
            finally {
                shutdownChannel(channel);
            }
            return samples;
        });
        long wallClockNanos = System.nanoTime() - startedAt;
        List<Long> latencies = flatten(workerSamples);
        LatencySummary summary = LatencySummary.fromNanos(latencies);
        assertThat(runtime.blockingStub().count(CountRequest.getDefaultInstance()).getCount()).isZero();
        return new ScenarioResult(
                "concurrent-unary-crud/defaults",
                runtime.label(),
                "rpc",
                totalOperations,
                summary.sampleCount(),
                summary.p50Millis(),
                summary.p95Millis(),
                summary.p99Millis(),
                throughput(totalOperations, wallClockNanos),
                "concurrency=%d; iterations_per_worker=%d".formatted(UNARY_CONCURRENCY, UNARY_ITERATIONS_PER_WORKER)
        );
    }

    private ScenarioResult measureLongRange(ServiceRuntime runtime, String prefix, int datasetSize, String scenarioName) {
        List<Long> streamDurations = new ArrayList<>();
        List<Long> firstItemDurations = new ArrayList<>();
        long totalWallClockNanos = 0L;
        for (int iteration = 0; iteration < RANGE_MEASURED_RUNS; iteration++) {
            long startedAt = System.nanoTime();
            var iterator = runtime.blockingStub().range(rangeRequest(prefix, datasetSize));
            int itemCount = 0;
            long firstItemAt = -1L;
            while (iterator.hasNext()) {
                RangeItem item = iterator.next();
                if (itemCount == 0) {
                    firstItemAt = System.nanoTime();
                }
                String expectedKey = rangeKey(prefix, itemCount);
                assertThat(item.getRecord().getKey()).isEqualTo(expectedKey);
                assertThat(item.getRecord().getValue().getData().toByteArray()).isEqualTo(payload(itemCount, VALUE_BYTES));
                itemCount++;
            }
            long finishedAt = System.nanoTime();
            assertThat(itemCount).isEqualTo(datasetSize);
            totalWallClockNanos += finishedAt - startedAt;
            streamDurations.add(finishedAt - startedAt);
            firstItemDurations.add((firstItemAt == -1L ? finishedAt : firstItemAt) - startedAt);
        }
        LatencySummary streamSummary = LatencySummary.fromNanos(streamDurations);
        LatencySummary firstItemSummary = LatencySummary.fromNanos(firstItemDurations);
        return new ScenarioResult(
                scenarioName,
                runtime.label(),
                "items",
                (long) datasetSize * RANGE_MEASURED_RUNS,
                streamSummary.sampleCount(),
                streamSummary.p50Millis(),
                streamSummary.p95Millis(),
                streamSummary.p99Millis(),
                throughput((long) datasetSize * RANGE_MEASURED_RUNS, totalWallClockNanos),
                "runs=%d; ttfi_p50_ms=%.3f".formatted(RANGE_MEASURED_RUNS, firstItemSummary.p50Millis())
        );
    }

    private ScenarioResult measureExactCount(ServiceRuntime runtime, int expectedCount, String scenarioName) {
        List<Long> durations = new ArrayList<>();
        long totalWallClockNanos = 0L;
        for (int iteration = 0; iteration < COUNT_MEASURED_RUNS; iteration++) {
            long startedAt = System.nanoTime();
            long actualCount = runtime.blockingStub().count(CountRequest.getDefaultInstance()).getCount();
            long finishedAt = System.nanoTime();
            assertThat(actualCount).isEqualTo(expectedCount);
            durations.add(finishedAt - startedAt);
            totalWallClockNanos += finishedAt - startedAt;
        }
        LatencySummary summary = LatencySummary.fromNanos(durations);
        return new ScenarioResult(
                scenarioName,
                runtime.label(),
                "rpc",
                COUNT_MEASURED_RUNS,
                summary.sampleCount(),
                summary.p50Millis(),
                summary.p95Millis(),
                summary.p99Millis(),
                throughput(COUNT_MEASURED_RUNS, totalWallClockNanos),
                "expected_count=%d".formatted(expectedCount)
        );
    }

    private ScenarioResult measureConcurrentRangeSaturation(
            ServiceRuntime runtime,
            String prefix,
            int datasetSize,
            String scenarioName
    ) throws Exception {
        long startedAt = System.nanoTime();
        List<StreamSaturationResult> streamResults = runtime.runConcurrent("range-saturation", SATURATION_REQUESTED_STREAMS,
                workerIndex -> () -> {
                    ManagedChannel channel = runtime.newChannel();
                    try {
                        KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel);
                        long streamStarted = System.nanoTime();
                        int itemCount = 0;
                        try {
                            var iterator = stub.range(rangeRequest(prefix, datasetSize));
                            while (iterator.hasNext()) {
                                iterator.next();
                                itemCount++;
                                if (itemCount % SATURATION_DELAY_EVERY_ITEMS == 0) {
                                    LockSupport.parkNanos(SATURATION_DELAY_NANOS);
                                }
                            }
                            assertThat(itemCount).isEqualTo(datasetSize);
                            return new StreamSaturationResult(true, false, System.nanoTime() - streamStarted, itemCount);
                        }
                        catch (StatusRuntimeException failure) {
                            if (failure.getStatus().getCode() != Status.Code.UNAVAILABLE) {
                                throw failure;
                            }
                            return new StreamSaturationResult(false, true, System.nanoTime() - streamStarted, 0);
                        }
                    }
                    finally {
                        shutdownChannel(channel);
                    }
                });
        long wallClockNanos = System.nanoTime() - startedAt;

        long successCount = streamResults.stream().filter(StreamSaturationResult::success).count();
        long rejectedCount = streamResults.stream().filter(StreamSaturationResult::rejected).count();
        long totalItems = streamResults.stream().mapToLong(StreamSaturationResult::itemCount).sum();
        List<Long> successDurations = streamResults.stream()
                .filter(StreamSaturationResult::success)
                .map(StreamSaturationResult::durationNanos)
                .toList();
        LatencySummary summary = LatencySummary.fromNanos(successDurations);
        return new ScenarioResult(
                scenarioName,
                runtime.label(),
                "items",
                totalItems,
                summary.sampleCount(),
                summary.p50Millis(),
                summary.p95Millis(),
                summary.p99Millis(),
                throughput(totalItems, wallClockNanos),
                "requested=%d; succeeded=%d; rejected=%d; reject_status=UNAVAILABLE"
                        .formatted(SATURATION_REQUESTED_STREAMS, successCount, rejectedCount)
        );
    }

    private RangeRequest rangeRequest(String prefix, int datasetSize) {
        return RangeRequest.newBuilder()
                .setKeySince(rangeKey(prefix, 0))
                .setKeyTo(rangeKey(prefix, datasetSize))
                .build();
    }

    private int consumeRange(KvServiceGrpc.KvServiceBlockingStub stub, RangeRequest request) {
        int itemCount = 0;
        var iterator = stub.range(request);
        while (iterator.hasNext()) {
            iterator.next();
            itemCount++;
        }
        return itemCount;
    }

    private PutRequest putRequest(String key, byte[] payload) {
        return PutRequest.newBuilder()
                .setKey(key)
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFrom(payload)).build())
                .build();
    }

    private static String bulkKey(int index) {
        return "bulk-%08d".formatted(index);
    }

    private static String unaryKey(String prefix, int workerIndex, int iteration) {
        return "%s-%02d-%06d".formatted(prefix, workerIndex, iteration);
    }

    private static String rangeKey(String prefix, int index) {
        return "%s-%08d".formatted(prefix, index);
    }

    private static byte[] payload(int index, int size) {
        byte[] payload = new byte[size];
        Arrays.fill(payload, (byte) ('a' + (index % 26)));
        payload[0] = (byte) ((index >>> 24) & 0xFF);
        payload[1] = (byte) ((index >>> 16) & 0xFF);
        payload[2] = (byte) ((index >>> 8) & 0xFF);
        payload[3] = (byte) (index & 0xFF);
        return payload;
    }

    private static double throughput(long units, long wallClockNanos) {
        if (units == 0 || wallClockNanos <= 0) {
            return 0.0d;
        }
        return units / (wallClockNanos / 1_000_000_000.0d);
    }

    private static List<Long> flatten(List<List<Long>> workerSamples) {
        List<Long> flattened = new ArrayList<>();
        for (List<Long> workerSample : workerSamples) {
            flattened.addAll(workerSample);
        }
        return flattened;
    }

    private Path writeReport(PerformanceReport report) throws IOException {
        Path reportDirectory = Path.of(System.getProperty(REPORT_DIR_PROPERTY, "target/perf-reports"));
        Files.createDirectories(reportDirectory);
        Path reportPath = reportDirectory.resolve("kvservice-performance-report.md");
        Files.writeString(reportPath, renderReport(report), StandardCharsets.UTF_8);
        return reportPath;
    }

    private String renderReport(PerformanceReport report) {
        StringBuilder builder = new StringBuilder();
        builder.append("# Performance evidence\n\n");
        builder.append("- Сформировано: ").append(report.generatedAt()).append('\n');
        builder.append("- Команда: `").append(report.command()).append("`\n");
        builder.append("- Canonical measurement mode: ").append(report.measurementMode()).append('\n');
        builder.append("- Среда: ").append(report.environment().describe()).append('\n');
        builder.append("- Tarantool image: `").append(TARANTOOL_IMAGE.asCanonicalNameString()).append("`\n\n");

        builder.append("## Evidence table\n\n");
        builder.append("| Сценарий | Конфиг | Единица | Объём | Samples | p50 ms | p95 ms | p99 ms | Throughput/s | Notes |\n");
        builder.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
        for (ScenarioResult result : report.results()) {
            builder.append("| ")
                    .append(result.scenario()).append(" | ")
                    .append(result.configLabel()).append(" | ")
                    .append(result.unitLabel()).append(" | ")
                    .append(result.totalUnits()).append(" | ")
                    .append(result.sampleCount()).append(" | ")
                    .append(formatDecimal(result.p50Millis())).append(" | ")
                    .append(formatDecimal(result.p95Millis())).append(" | ")
                    .append(formatDecimal(result.p99Millis())).append(" | ")
                    .append(formatDecimal(result.throughputPerSecond())).append(" | ")
                    .append(result.notes()).append(" |\n");
        }

        builder.append("\n## Defaults review inputs\n\n");
        for (String reviewLine : report.defaultsReview()) {
            builder.append("- ").append(reviewLine).append('\n');
        }

        builder.append("\n## Caveats\n\n");
        for (String caveat : report.caveats()) {
            builder.append("- ").append(caveat).append('\n');
        }
        return builder.toString();
    }

    private String formatDecimal(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static void shutdownChannel(ManagedChannel channel) throws InterruptedException {
        if (channel == null) {
            return;
        }
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
    }

    private enum BulkStage {
        PUT,
        GET,
        DELETE
    }

    private record ScenarioResult(
            String scenario,
            String configLabel,
            String unitLabel,
            long totalUnits,
            int sampleCount,
            double p50Millis,
            double p95Millis,
            double p99Millis,
            double throughputPerSecond,
            String notes
    ) {
    }

    private record StreamSaturationResult(boolean success, boolean rejected, long durationNanos, int itemCount) {
    }

    private record PerformanceReport(
            OffsetDateTime generatedAt,
            String command,
            String measurementMode,
            EnvironmentSnapshot environment,
            List<ScenarioResult> results,
            List<String> defaultsReview,
            List<String> caveats
    ) {
    }

    private record EnvironmentSnapshot(
            String osName,
            String osVersion,
            String osArch,
            String javaVersion,
            int availableProcessors,
            long maxHeapMegabytes
    ) {

        static EnvironmentSnapshot capture() {
            long maxHeapBytes = Runtime.getRuntime().maxMemory();
            return new EnvironmentSnapshot(
                    System.getProperty("os.name"),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.version"),
                    Runtime.getRuntime().availableProcessors(),
                    maxHeapBytes <= 0 ? -1 : maxHeapBytes / (1024 * 1024)
            );
        }

        String describe() {
            return "%s %s (%s), Java %s, cpu=%d, maxHeapMiB=%d, jvm=%s".formatted(
                    this.osName,
                    this.osVersion,
                    this.osArch,
                    this.javaVersion,
                    this.availableProcessors,
                    this.maxHeapMegabytes,
                    ManagementFactory.getRuntimeMXBean().getVmName()
            );
        }
    }

    private record LatencySummary(int sampleCount, double p50Millis, double p95Millis, double p99Millis) {

        static LatencySummary fromNanos(List<Long> durations) {
            Objects.requireNonNull(durations, "durations");
            if (durations.isEmpty()) {
                return new LatencySummary(0, 0.0d, 0.0d, 0.0d);
            }
            List<Long> sorted = new ArrayList<>(durations);
            Collections.sort(sorted);
            return new LatencySummary(
                    sorted.size(),
                    nanosToMillis(percentile(sorted, 0.50d)),
                    nanosToMillis(percentile(sorted, 0.95d)),
                    nanosToMillis(percentile(sorted, 0.99d))
            );
        }

        private static long percentile(List<Long> sorted, double percentile) {
            int index = (int) Math.ceil(percentile * sorted.size()) - 1;
            int boundedIndex = Math.max(0, Math.min(index, sorted.size() - 1));
            return sorted.get(boundedIndex);
        }

        private static double nanosToMillis(long nanos) {
            return nanos / 1_000_000.0d;
        }
    }

    private static final class ServiceRuntime implements AutoCloseable {

        private final String label;
        private final ConfigurableApplicationContext context;
        private final ManagedChannel channel;
        private final KvServiceGrpc.KvServiceBlockingStub blockingStub;
        private final TarantoolBoxClient tarantoolClient;

        private ServiceRuntime(
                String label,
                ConfigurableApplicationContext context,
                ManagedChannel channel,
                KvServiceGrpc.KvServiceBlockingStub blockingStub,
                TarantoolBoxClient tarantoolClient
        ) {
            this.label = label;
            this.context = context;
            this.channel = channel;
            this.blockingStub = blockingStub;
            this.tarantoolClient = tarantoolClient;
        }

        static ServiceRuntime start(String label, Map<String, String> overrides) {
            if (!TARANTOOL.isRunning()) {
                TARANTOOL.start();
            }
            ConfigurableApplicationContext context = new SpringApplicationBuilder(KvServiceApplication.class)
                    .run(baseArguments(overrides));
            int grpcPort = context.getBean(GrpcServerLifecycle.class).getPort();
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                    .usePlaintext()
                    .build();
            return new ServiceRuntime(
                    label,
                    context,
                    channel,
                    KvServiceGrpc.newBlockingStub(channel),
                    context.getBean(TarantoolBoxClient.class)
            );
        }

        private static String[] baseArguments(Map<String, String> overrides) {
            Map<String, String> properties = new java.util.LinkedHashMap<>();
            properties.put("server.port", "0");
            properties.put("spring.grpc.server.port", "0");
            properties.put("management.tracing.export.enabled", "false");
            properties.put("kvservice.tarantool.host", TARANTOOL.getHost());
            properties.put("kvservice.tarantool.port", Integer.toString(TARANTOOL.getMappedPort(TARANTOOL_PORT)));
            properties.put("kvservice.tarantool.username", USERNAME);
            properties.put("kvservice.tarantool.password", PASSWORD);
            properties.putAll(overrides);
            return properties.entrySet().stream()
                    .map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                    .toArray(String[]::new);
        }

        String label() {
            return this.label;
        }

        KvServiceGrpc.KvServiceBlockingStub blockingStub() {
            return this.blockingStub;
        }

        ManagedChannel newChannel() {
            int grpcPort = this.context.getBean(GrpcServerLifecycle.class).getPort();
            return ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                    .usePlaintext()
                    .build();
        }

        void truncateSpace() throws Exception {
            this.tarantoolClient.eval("if box.space.KV ~= nil then box.space.KV:truncate() end return true")
                    .get(5, TimeUnit.SECONDS);
        }

        void seedRangeDatasetDirect(String prefix, int datasetSize, int valueBytes) throws Exception {
            TarantoolBoxSpace space = this.tarantoolClient.space(SPACE_NAME);
            BaseOptions options = BaseOptions.builder().withTimeout(5_000L).build();
            for (int index = 0; index < datasetSize; index++) {
                space.replace(List.of(rangeKey(prefix, index), payload(index, valueBytes)), options)
                        .get(5, TimeUnit.SECONDS);
            }
        }

        <T> List<T> runConcurrent(String name, int concurrency, WorkerFactory<T> workerFactory) throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            CountDownLatch startGate = new CountDownLatch(1);
            try {
                List<Future<T>> futures = new ArrayList<>();
                for (int workerIndex = 0; workerIndex < concurrency; workerIndex++) {
                    int capturedIndex = workerIndex;
                    futures.add(executor.submit(() -> {
                        startGate.await(10, TimeUnit.SECONDS);
                        return workerFactory.create(capturedIndex).call();
                    }));
                }
                startGate.countDown();
                List<T> results = new ArrayList<>(concurrency);
                for (Future<T> future : futures) {
                    results.add(future.get(5, TimeUnit.MINUTES));
                }
                return results;
            }
            finally {
                executor.shutdownNow();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            }
        }

        @Override
        public void close() throws Exception {
            shutdownChannel(this.channel);
            this.context.close();
        }
    }

    @FunctionalInterface
    private interface WorkerFactory<T> {

        java.util.concurrent.Callable<T> create(int workerIndex) throws Exception;
    }
}
