package io.kvservice.perf;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.micrometer.core.instrument.MeterRegistry;
import io.tarantool.client.BaseOptions;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
class KvServiceHeavyScaleIT {

  private static final String USERNAME = "kvservice";
  private static final String PASSWORD = "kvservice";
  private static final int TARANTOOL_PORT = 3301;
  private static final DockerImageName TARANTOOL_IMAGE =
      DockerImageName.parse("tarantool/tarantool:3.2.1");
  private static final String REPORT_DIR_PROPERTY = "kvservice.perf.report-dir";
  private static final String PERF_COMMAND_PROPERTY = "kvservice.perf.command";
  private static final String SPACE_NAME = "KV";
  private static final String HEAVY_PREFIX = "heavy-scale";
  private static final String TEMP_PREFIX = "heavy-temp";
  private static final String SEED_SCRIPT_PATH = "scripts/perf-heavy-seed.lua";
  private static final String CANONICAL_MEASUREMENT_MODE =
      "management.tracing.export.enabled=false; внешний OTLP backend не используется; metrics/logging остаются включены";
  private static final long MAX_DATASET_SIZE = 5_000_000L;
  private static final String RANGE_ACTIVE_METRIC = "kvservice.grpc.range.stream.active";

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

  @Test
  void generatesHeavyScaleEvidence() throws Exception {
    HeavyConfig config = HeavyConfig.fromSystemProperties();
    List<ScenarioResult> results = new ArrayList<>();
    List<PhaseSnapshot> phaseSnapshots = new ArrayList<>();

    SeedSummary seedSummary;
    CountDefaultProbe countDefaultProbe;

    try (ServiceRuntime runtime = ServiceRuntime.start("heavy-defaults", Map.of())) {
      runtime.truncateSpace();
      phaseSnapshots.add(runtime.capturePhaseSnapshot("startup-empty"));

      seedSummary = seedLargeDataset(runtime, HEAVY_PREFIX, config);
      validateSeededDataset(runtime, HEAVY_PREFIX, config);
      phaseSnapshots.add(runtime.capturePhaseSnapshot("post-seed"));

      results.add(measureLongRange(runtime, HEAVY_PREFIX, config));
      results.add(measureExistingGetProbes(runtime, HEAVY_PREFIX, config));
      results.add(measureTempCrudProbes(runtime, config));
      results.add(measureRangeGuardrailProbe(runtime, HEAVY_PREFIX, config));
      phaseSnapshots.add(runtime.capturePhaseSnapshot("post-default-scenarios"));

      countDefaultProbe = probeCountUnderCurrentDefault(runtime, config);
      phaseSnapshots.add(runtime.capturePhaseSnapshot("post-default-count-probe"));
    }

    try (ServiceRuntime runtime =
        ServiceRuntime.start(
            "count-completion-request-timeout-%ss"
                .formatted(config.completionRequestTimeout().toSeconds()),
            Map.of(
                "kvservice.tarantool.request-timeout",
                config.completionRequestTimeout().toString()))) {
      phaseSnapshots.add(runtime.capturePhaseSnapshot("startup-count-completion"));
      results.add(measureExactCount(runtime, config, countDefaultProbe));
      phaseSnapshots.add(runtime.capturePhaseSnapshot("post-count-completion"));

      assertThat(
              runtime
                  .blockingStub()
                  .withDeadlineAfter(config.countClientDeadline().toSeconds(), TimeUnit.SECONDS)
                  .count(CountRequest.getDefaultInstance())
                  .getCount())
          .isEqualTo(config.datasetSize());
    }

    HeavyScaleReport report =
        new HeavyScaleReport(
            OffsetDateTime.now(),
            System.getProperty(PERF_COMMAND_PROPERTY, "./mvnw -Pperf-heavy verify"),
            CANONICAL_MEASUREMENT_MODE,
            EnvironmentSnapshot.capture(),
            config,
            seedSummary,
            results,
            phaseSnapshots,
            List.of(
                "Любой runtime default пересматривается только после серии минимум из 3 controlled heavy-run сравнения current default и candidate в одном и том же measurement mode.",
                "Candidate допускается к смене default только если одновременно показывает устойчивое преимущество или не хуже текущего значения по throughput и latency/TTFI, не ухудшает guardrail behavior и не создаёт заметно больший resource pressure по bounded snapshots.",
                "Если heavy evidence неоднозначен или держится на single-run winner, defaults не меняются."),
            List.of(
                "Heavy workflow дополняет `./mvnw -Pperf verify`, а не заменяет его: быстрый local baseline и heavy-scale proof intentionally разведены.",
                "Exact `count()` измеряется с отдельным client-side cap, чтобы large-dataset run завершался полностью; verdict по server default всё равно опирается на observed latency и отдельный probe под текущим default guardrail.",
                "Все числа относятся к одному host и bounded workflow; это не capacity promise и не PR gate."));

    Path reportPath = writeReport(report);

    assertThat(reportPath).exists();
    assertThat(seedSummary.totalRecords()).isEqualTo(config.datasetSize());
    assertThat(results)
        .extracting(ScenarioResult::scenario)
        .containsExactly(
            "long-range/windowed-stream",
            "unary-probes/get-hit",
            "unary-probes/temp-put-get-delete",
            "range-guardrail/default-max-streams",
            "count/exact-completion");
  }

  private SeedSummary seedLargeDataset(ServiceRuntime runtime, String prefix, HeavyConfig config)
      throws Exception {
    String seedScript = Files.readString(Path.of(SEED_SCRIPT_PATH), StandardCharsets.UTF_8);
    AtomicLong nextStart = new AtomicLong(0L);
    long totalBatches = Math.ceilDiv(config.datasetSize(), config.seedBatchSize());
    BaseOptions options =
        BaseOptions.builder().withTimeout(config.seedBatchTimeout().toMillis()).build();
    long startedAt = System.nanoTime();

    ExecutorService executor = Executors.newFixedThreadPool(config.seedConcurrency());
    try {
      List<Future<Long>> futures = new ArrayList<>();
      for (int workerIndex = 0; workerIndex < config.seedConcurrency(); workerIndex++) {
        futures.add(
            executor.submit(
                () -> {
                  long inserted = 0L;
                  try (TarantoolBoxClient client = newDirectClient()) {
                    while (true) {
                      long batchStart = nextStart.getAndAdd(config.seedBatchSize());
                      if (batchStart >= config.datasetSize()) {
                        break;
                      }
                      long batchEnd =
                          Math.min(config.datasetSize(), batchStart + config.seedBatchSize());
                      inserted +=
                          executeSeedBatch(
                              client,
                              seedScript,
                              prefix,
                              batchStart,
                              batchEnd,
                              config.valueBytes(),
                              options);
                    }
                  }
                  return inserted;
                }));
      }

      long totalInserted = 0L;
      for (Future<Long> future : futures) {
        totalInserted += future.get(config.seedOverallTimeout().toMinutes(), TimeUnit.MINUTES);
      }
      assertThat(totalInserted).isEqualTo(config.datasetSize());
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    long finishedAt = System.nanoTime();
    return new SeedSummary(
        prefix,
        config.datasetSize(),
        config.seedBatchSize(),
        config.seedConcurrency(),
        totalBatches,
        Duration.ofNanos(finishedAt - startedAt),
        throughput(config.datasetSize(), finishedAt - startedAt),
        SEED_SCRIPT_PATH);
  }

  private long executeSeedBatch(
      TarantoolBoxClient client,
      String seedScript,
      String prefix,
      long startInclusive,
      long endExclusive,
      int valueBytes,
      BaseOptions options)
      throws Exception {
    List<?> response =
        client
            .eval(seedScript, List.of(prefix, startInclusive, endExclusive, valueBytes), options)
            .get(options.getTimeout(), TimeUnit.MILLISECONDS)
            .get();
    return ((Number) response.getFirst()).longValue();
  }

  private void validateSeededDataset(ServiceRuntime runtime, String prefix, HeavyConfig config) {
    Set<Long> probeIndexes = new LinkedHashSet<>();
    probeIndexes.add(0L);
    probeIndexes.add(config.datasetSize() / 2);
    probeIndexes.add(config.datasetSize() - 1);
    for (long index : probeIndexes) {
      byte[] actual =
          runtime
              .blockingStub()
              .get(GetRequest.newBuilder().setKey(rangeKey(prefix, index)).build())
              .getRecord()
              .getValue()
              .getData()
              .toByteArray();
      assertThat(actual).isEqualTo(seedPayload(index, config.valueBytes()));
    }
  }

  private CountDefaultProbe probeCountUnderCurrentDefault(
      ServiceRuntime runtime, HeavyConfig config) {
    long startedAt = System.nanoTime();
    try {
      long actual = runtime.blockingStub().count(CountRequest.getDefaultInstance()).getCount();
      long finishedAt = System.nanoTime();
      assertThat(actual).isEqualTo(config.datasetSize());
      return CountDefaultProbe.success(finishedAt - startedAt);
    } catch (StatusRuntimeException failure) {
      Status.Code statusCode = failure.getStatus().getCode();
      if (statusCode != Status.Code.DEADLINE_EXCEEDED && statusCode != Status.Code.UNAVAILABLE) {
        throw failure;
      }
      return CountDefaultProbe.failed(statusCode, System.nanoTime() - startedAt);
    }
  }

  private ScenarioResult measureExactCount(
      ServiceRuntime runtime, HeavyConfig config, CountDefaultProbe defaultProbe) {
    List<Long> durations = new ArrayList<>();
    long totalWallClockNanos = 0L;
    try (ResourceMonitor monitor = new ResourceMonitor(runtime)) {
      KvServiceGrpc.KvServiceBlockingStub stub =
          runtime
              .blockingStub()
              .withDeadlineAfter(config.countClientDeadline().toSeconds(), TimeUnit.SECONDS);
      for (int iteration = 0; iteration < config.countMeasuredRuns(); iteration++) {
        long startedAt = System.nanoTime();
        long actualCount = stub.count(CountRequest.getDefaultInstance()).getCount();
        long finishedAt = System.nanoTime();
        assertThat(actualCount).isEqualTo(config.datasetSize());
        durations.add(finishedAt - startedAt);
        totalWallClockNanos += finishedAt - startedAt;
      }
      LatencySummary summary = LatencySummary.fromNanos(durations);
      ScenarioResources resources = monitor.snapshot();
      return new ScenarioResult(
          "count/exact-completion",
          runtime.label(),
          "rpc",
          config.countMeasuredRuns(),
          summary.sampleCount(),
          summary.p50Millis(),
          summary.p95Millis(),
          summary.p99Millis(),
          throughput(config.countMeasuredRuns(), totalWallClockNanos),
          resources.peakHeapMiB(),
          resources.maxActiveRangeStreams(),
          "expected_count=%d; client_deadline=%ds; completion_request_timeout=%s; default_guardrail=%s"
              .formatted(
                  config.datasetSize(),
                  config.countClientDeadline().toSeconds(),
                  config.completionRequestTimeout(),
                  defaultProbe.describe()));
    }
  }

  private ScenarioResult measureLongRange(
      ServiceRuntime runtime, String prefix, HeavyConfig config) {
    long windowStart = (config.datasetSize() - config.rangeWindowSize()) / 2;
    List<Long> streamDurations = new ArrayList<>();
    List<Long> firstItemDurations = new ArrayList<>();
    long totalWallClockNanos = 0L;
    try (ResourceMonitor monitor = new ResourceMonitor(runtime)) {
      KvServiceGrpc.KvServiceBlockingStub stub =
          runtime
              .blockingStub()
              .withDeadlineAfter(config.rangeClientDeadline().toSeconds(), TimeUnit.SECONDS);
      for (int iteration = 0; iteration < config.rangeMeasuredRuns(); iteration++) {
        long startedAt = System.nanoTime();
        var iterator =
            stub.range(rangeRequest(prefix, windowStart, windowStart + config.rangeWindowSize()));
        long firstItemAt = -1L;
        long expectedIndex = windowStart;
        long itemCount = 0L;
        while (iterator.hasNext()) {
          RangeItem item = iterator.next();
          if (itemCount == 0L) {
            firstItemAt = System.nanoTime();
          }
          assertThat(item.getRecord().getKey()).isEqualTo(rangeKey(prefix, expectedIndex));
          assertThat(item.getRecord().getValue().getData().toByteArray())
              .isEqualTo(seedPayload(expectedIndex, config.valueBytes()));
          itemCount++;
          expectedIndex++;
        }
        long finishedAt = System.nanoTime();
        assertThat(itemCount).isEqualTo(config.rangeWindowSize());
        streamDurations.add(finishedAt - startedAt);
        firstItemDurations.add((firstItemAt == -1L ? finishedAt : firstItemAt) - startedAt);
        totalWallClockNanos += finishedAt - startedAt;
      }
      LatencySummary completionSummary = LatencySummary.fromNanos(streamDurations);
      LatencySummary ttfiSummary = LatencySummary.fromNanos(firstItemDurations);
      ScenarioResources resources = monitor.snapshot();
      return new ScenarioResult(
          "long-range/windowed-stream",
          runtime.label(),
          "items",
          config.rangeWindowSize() * config.rangeMeasuredRuns(),
          completionSummary.sampleCount(),
          completionSummary.p50Millis(),
          completionSummary.p95Millis(),
          completionSummary.p99Millis(),
          throughput(config.rangeWindowSize() * config.rangeMeasuredRuns(), totalWallClockNanos),
          resources.peakHeapMiB(),
          resources.maxActiveRangeStreams(),
          String.format(
              Locale.ROOT,
              "window_start=%d; window_size=%d; ttfi_p50_ms=%.3f; client_deadline=%ds",
              windowStart,
              config.rangeWindowSize(),
              ttfiSummary.p50Millis(),
              config.rangeClientDeadline().toSeconds()));
    }
  }

  private ScenarioResult measureExistingGetProbes(
      ServiceRuntime runtime, String prefix, HeavyConfig config) throws Exception {
    int probeCount =
        Math.toIntExact(Math.min(config.existingGetProbeCount(), config.datasetSize()));
    List<Long> probeIndexes = evenlyDistributedIndexes(probeCount, config.datasetSize());
    long startedAt = System.nanoTime();
    try (ResourceMonitor monitor = new ResourceMonitor(runtime)) {
      List<List<Long>> workerSamples =
          runtime.runConcurrent(
              "heavy-get-hit",
              config.unaryConcurrency(),
              workerIndex ->
                  () -> {
                    List<Long> samples = new ArrayList<>();
                    ManagedChannel channel = runtime.newChannel();
                    try {
                      KvServiceGrpc.KvServiceBlockingStub stub =
                          KvServiceGrpc.newBlockingStub(channel);
                      for (int index = workerIndex;
                          index < probeIndexes.size();
                          index += config.unaryConcurrency()) {
                        long recordIndex = probeIndexes.get(index);
                        long operationStarted = System.nanoTime();
                        byte[] actual =
                            stub.get(
                                    GetRequest.newBuilder()
                                        .setKey(rangeKey(prefix, recordIndex))
                                        .build())
                                .getRecord()
                                .getValue()
                                .getData()
                                .toByteArray();
                        samples.add(System.nanoTime() - operationStarted);
                        assertThat(actual).isEqualTo(seedPayload(recordIndex, config.valueBytes()));
                      }
                    } finally {
                      shutdownChannel(channel);
                    }
                    return samples;
                  });
      long wallClockNanos = System.nanoTime() - startedAt;
      LatencySummary summary = LatencySummary.fromNanos(flatten(workerSamples));
      ScenarioResources resources = monitor.snapshot();
      return new ScenarioResult(
          "unary-probes/get-hit",
          runtime.label(),
          "rpc",
          probeIndexes.size(),
          summary.sampleCount(),
          summary.p50Millis(),
          summary.p95Millis(),
          summary.p99Millis(),
          throughput(probeIndexes.size(), wallClockNanos),
          resources.peakHeapMiB(),
          resources.maxActiveRangeStreams(),
          "probe_count=%d; distribution=evenly-spaced; concurrency=%d"
              .formatted(probeIndexes.size(), config.unaryConcurrency()));
    }
  }

  private ScenarioResult measureTempCrudProbes(ServiceRuntime runtime, HeavyConfig config)
      throws Exception {
    long totalOperations = (long) config.unaryTempCrudProbeCount() * 3L;
    long startedAt = System.nanoTime();
    try (ResourceMonitor monitor = new ResourceMonitor(runtime)) {
      List<List<Long>> workerSamples =
          runtime.runConcurrent(
              "heavy-temp-crud",
              config.unaryConcurrency(),
              workerIndex ->
                  () -> {
                    List<Long> samples = new ArrayList<>();
                    ManagedChannel channel = runtime.newChannel();
                    try {
                      KvServiceGrpc.KvServiceBlockingStub stub =
                          KvServiceGrpc.newBlockingStub(channel);
                      for (int index = workerIndex;
                          index < config.unaryTempCrudProbeCount();
                          index += config.unaryConcurrency()) {
                        String key = tempKey(workerIndex, index);
                        byte[] payload =
                            seedPayload(config.datasetSize() + index, config.valueBytes());

                        long putStarted = System.nanoTime();
                        stub.put(putRequest(key, payload));
                        samples.add(System.nanoTime() - putStarted);

                        long getStarted = System.nanoTime();
                        byte[] actual =
                            stub.get(GetRequest.newBuilder().setKey(key).build())
                                .getRecord()
                                .getValue()
                                .getData()
                                .toByteArray();
                        samples.add(System.nanoTime() - getStarted);
                        assertThat(actual).isEqualTo(payload);

                        long deleteStarted = System.nanoTime();
                        stub.delete(DeleteRequest.newBuilder().setKey(key).build());
                        samples.add(System.nanoTime() - deleteStarted);
                      }
                    } finally {
                      shutdownChannel(channel);
                    }
                    return samples;
                  });
      long wallClockNanos = System.nanoTime() - startedAt;
      LatencySummary summary = LatencySummary.fromNanos(flatten(workerSamples));
      ScenarioResources resources = monitor.snapshot();
      return new ScenarioResult(
          "unary-probes/temp-put-get-delete",
          runtime.label(),
          "rpc",
          totalOperations,
          summary.sampleCount(),
          summary.p50Millis(),
          summary.p95Millis(),
          summary.p99Millis(),
          throughput(totalOperations, wallClockNanos),
          resources.peakHeapMiB(),
          resources.maxActiveRangeStreams(),
          "probe_keys=%s; concurrency=%d; logical_items=%d"
              .formatted(TEMP_PREFIX, config.unaryConcurrency(), config.unaryTempCrudProbeCount()));
    }
  }

  private ScenarioResult measureRangeGuardrailProbe(
      ServiceRuntime runtime, String prefix, HeavyConfig config) throws Exception {
    long startedAt = System.nanoTime();
    long windowStart = config.datasetSize() - config.guardrailWindowSize();
    try (ResourceMonitor monitor = new ResourceMonitor(runtime)) {
      List<StreamSaturationResult> streamResults =
          runtime.runConcurrent(
              "heavy-range-guardrail",
              config.guardrailRequestedStreams(),
              workerIndex ->
                  () -> {
                    ManagedChannel channel = runtime.newChannel();
                    try {
                      KvServiceGrpc.KvServiceBlockingStub stub =
                          KvServiceGrpc.newBlockingStub(channel)
                              .withDeadlineAfter(
                                  config.rangeClientDeadline().toSeconds(), TimeUnit.SECONDS);
                      long streamStarted = System.nanoTime();
                      long itemCount = 0L;
                      try {
                        var iterator =
                            stub.range(
                                rangeRequest(
                                    prefix,
                                    windowStart,
                                    windowStart + config.guardrailWindowSize()));
                        while (iterator.hasNext()) {
                          iterator.next();
                          itemCount++;
                        }
                        assertThat(itemCount).isEqualTo(config.guardrailWindowSize());
                        return new StreamSaturationResult(
                            true, false, System.nanoTime() - streamStarted, itemCount);
                      } catch (StatusRuntimeException failure) {
                        if (failure.getStatus().getCode() != Status.Code.UNAVAILABLE) {
                          throw failure;
                        }
                        return new StreamSaturationResult(
                            false, true, System.nanoTime() - streamStarted, 0L);
                      }
                    } finally {
                      shutdownChannel(channel);
                    }
                  });
      long wallClockNanos = System.nanoTime() - startedAt;
      long successCount = streamResults.stream().filter(StreamSaturationResult::success).count();
      long rejectedCount = streamResults.stream().filter(StreamSaturationResult::rejected).count();
      long totalItems = streamResults.stream().mapToLong(StreamSaturationResult::itemCount).sum();
      List<Long> successDurations =
          streamResults.stream()
              .filter(StreamSaturationResult::success)
              .map(StreamSaturationResult::durationNanos)
              .toList();
      LatencySummary summary = LatencySummary.fromNanos(successDurations);
      ScenarioResources resources = monitor.snapshot();
      return new ScenarioResult(
          "range-guardrail/default-max-streams",
          runtime.label(),
          "items",
          totalItems,
          summary.sampleCount(),
          summary.p50Millis(),
          summary.p95Millis(),
          summary.p99Millis(),
          throughput(totalItems, wallClockNanos),
          resources.peakHeapMiB(),
          resources.maxActiveRangeStreams(),
          "requested=%d; succeeded=%d; rejected=%d; reject_status=UNAVAILABLE; window_size=%d"
              .formatted(
                  config.guardrailRequestedStreams(),
                  successCount,
                  rejectedCount,
                  config.guardrailWindowSize()));
    }
  }

  private RangeRequest rangeRequest(String prefix, long startInclusive, long endExclusive) {
    return RangeRequest.newBuilder()
        .setKeySince(rangeKey(prefix, startInclusive))
        .setKeyTo(rangeKey(prefix, endExclusive))
        .build();
  }

  private PutRequest putRequest(String key, byte[] payload) {
    return PutRequest.newBuilder()
        .setKey(key)
        .setValue(NullableBytes.newBuilder().setData(ByteString.copyFrom(payload)).build())
        .build();
  }

  private static List<Long> evenlyDistributedIndexes(int sampleCount, long datasetSize) {
    if (sampleCount <= 0) {
      return List.of();
    }
    if (sampleCount == 1) {
      return List.of(0L);
    }
    List<Long> indexes = new ArrayList<>(sampleCount);
    long maxIndex = datasetSize - 1;
    for (int sample = 0; sample < sampleCount; sample++) {
      long index = Math.round((double) sample * maxIndex / (sampleCount - 1));
      indexes.add(index);
    }
    return indexes;
  }

  private static String rangeKey(String prefix, long index) {
    return "%s-%08d".formatted(prefix, index);
  }

  private static String tempKey(int workerIndex, int iteration) {
    return "%s-%02d-%06d".formatted(TEMP_PREFIX, workerIndex, iteration);
  }

  private static byte[] seedPayload(long index, int valueBytes) {
    String marker = "%08d:".formatted(index);
    if (valueBytes <= marker.length()) {
      return marker.substring(0, valueBytes).getBytes(StandardCharsets.UTF_8);
    }
    char fill = (char) ('A' + (int) (index % 26));
    return (marker + String.valueOf(fill).repeat(valueBytes - marker.length()))
        .getBytes(StandardCharsets.UTF_8);
  }

  private static double throughput(long units, long wallClockNanos) {
    if (units == 0L || wallClockNanos <= 0L) {
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

  private Path writeReport(HeavyScaleReport report) throws IOException {
    Path reportDirectory =
        Path.of(System.getProperty(REPORT_DIR_PROPERTY, "target/perf-heavy-reports"));
    Files.createDirectories(reportDirectory);
    Path reportPath = reportDirectory.resolve("kvservice-heavy-scale-report.md");
    Files.writeString(reportPath, renderReport(report), StandardCharsets.UTF_8);
    return reportPath;
  }

  private String renderReport(HeavyScaleReport report) {
    StringBuilder builder = new StringBuilder();
    builder.append("# Heavy-scale performance evidence\n\n");
    builder.append("- Сформировано: ").append(report.generatedAt()).append('\n');
    builder.append("- Команда: `").append(report.command()).append("`\n");
    builder.append("- Canonical measurement mode: ").append(report.measurementMode()).append('\n');
    builder.append("- Среда: ").append(report.environment().describe()).append('\n');
    builder
        .append("- Tarantool image: `")
        .append(TARANTOOL_IMAGE.asCanonicalNameString())
        .append("`\n\n");

    builder.append("## Heavy workflow config\n\n");
    builder.append("- dataset_size=").append(report.config().datasetSize()).append('\n');
    builder.append("- value_bytes=").append(report.config().valueBytes()).append('\n');
    builder.append("- range_window_size=").append(report.config().rangeWindowSize()).append('\n');
    builder
        .append("- guardrail_window_size=")
        .append(report.config().guardrailWindowSize())
        .append('\n');
    builder
        .append("- count_client_deadline=")
        .append(report.config().countClientDeadline())
        .append('\n');
    builder
        .append("- range_client_deadline=")
        .append(report.config().rangeClientDeadline())
        .append("\n\n");

    builder.append("## Dataset preparation\n\n");
    builder
        .append("- Mode: direct Tarantool eval batches via `")
        .append(report.seedSummary().scriptPath())
        .append("`\n");
    builder.append("- Prefix: `").append(report.seedSummary().prefix()).append("`\n");
    builder.append("- Records: ").append(report.seedSummary().totalRecords()).append('\n');
    builder.append("- Batch size: ").append(report.seedSummary().batchSize()).append('\n');
    builder.append("- Concurrency: ").append(report.seedSummary().concurrency()).append('\n');
    builder.append("- Batches: ").append(report.seedSummary().batchCount()).append('\n');
    builder.append("- Wall clock: ").append(report.seedSummary().wallClock()).append('\n');
    builder
        .append("- Throughput/s: ")
        .append(formatDecimal(report.seedSummary().throughputPerSecond()))
        .append("\n\n");

    builder.append("## Evidence table\n\n");
    builder.append(
        "| Сценарий | Конфиг | Единица | Объём | Samples | p50 ms | p95 ms | p99 ms | Throughput/s | Peak JVM MiB | Max active range | Notes |\n");
    builder.append(
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
    for (ScenarioResult result : report.results()) {
      builder
          .append("| ")
          .append(result.scenario())
          .append(" | ")
          .append(result.configLabel())
          .append(" | ")
          .append(result.unitLabel())
          .append(" | ")
          .append(result.totalUnits())
          .append(" | ")
          .append(result.sampleCount())
          .append(" | ")
          .append(formatDecimal(result.p50Millis()))
          .append(" | ")
          .append(formatDecimal(result.p95Millis()))
          .append(" | ")
          .append(formatDecimal(result.p99Millis()))
          .append(" | ")
          .append(formatDecimal(result.throughputPerSecond()))
          .append(" | ")
          .append(formatDecimal(result.peakHeapMiB()))
          .append(" | ")
          .append(formatDecimal(result.maxActiveRangeStreams()))
          .append(" | ")
          .append(result.notes())
          .append(" |\n");
    }

    builder.append("\n## Phase snapshots\n\n");
    builder.append(
        "| Phase | JVM heap MiB | JVM non-heap MiB | Tarantool cgroup MiB | Active range streams |\n");
    builder.append("| --- | ---: | ---: | ---: | ---: |\n");
    for (PhaseSnapshot snapshot : report.phaseSnapshots()) {
      builder
          .append("| ")
          .append(snapshot.phase())
          .append(" | ")
          .append(formatBytesInMiB(snapshot.heapUsedBytes()))
          .append(" | ")
          .append(formatBytesInMiB(snapshot.nonHeapUsedBytes()))
          .append(" | ")
          .append(formatBytesInMiB(snapshot.tarantoolMemoryBytes()))
          .append(" | ")
          .append(formatDecimal(snapshot.activeRangeStreams()))
          .append(" |\n");
    }

    builder.append("\n## Defaults decision rule\n\n");
    for (String line : report.decisionRule()) {
      builder.append("- ").append(line).append('\n');
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

  private String formatBytesInMiB(long bytes) {
    if (bytes < 0L) {
      return "n/a";
    }
    return formatDecimal(bytes / (1024.0d * 1024.0d));
  }

  private static void shutdownChannel(ManagedChannel channel) throws InterruptedException {
    if (channel == null) {
      return;
    }
    channel.shutdownNow();
    channel.awaitTermination(5, TimeUnit.SECONDS);
  }

  private static TarantoolBoxClient newDirectClient() throws Exception {
    if (!TARANTOOL.isRunning()) {
      TARANTOOL.start();
    }
    return TarantoolFactory.box()
        .withHost(TARANTOOL.getHost())
        .withPort(TARANTOOL.getMappedPort(TARANTOOL_PORT))
        .withUser(USERNAME)
        .withPassword(PASSWORD)
        .withConnectTimeout(Duration.ofSeconds(3).toMillis())
        .withReconnectAfter(Duration.ofMillis(100).toMillis())
        .withFetchSchema(true)
        .build();
  }

  private record SeedSummary(
      String prefix,
      long totalRecords,
      int batchSize,
      int concurrency,
      long batchCount,
      Duration wallClock,
      double throughputPerSecond,
      String scriptPath) {}

  private record CountDefaultProbe(Status.Code statusCode, long durationNanos) {

    static CountDefaultProbe success(long durationNanos) {
      return new CountDefaultProbe(Status.Code.OK, durationNanos);
    }

    static CountDefaultProbe failed(Status.Code statusCode, long durationNanos) {
      return new CountDefaultProbe(statusCode, durationNanos);
    }

    String describe() {
      if (this.statusCode == Status.Code.OK) {
        return String.format(Locale.ROOT, "success@%.3fms", this.durationNanos / 1_000_000.0d);
      }
      return String.format(
          Locale.ROOT, "%s@%.3fms", this.statusCode.name(), this.durationNanos / 1_000_000.0d);
    }
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
      double peakHeapMiB,
      double maxActiveRangeStreams,
      String notes) {}

  private record PhaseSnapshot(
      String phase,
      long heapUsedBytes,
      long nonHeapUsedBytes,
      long tarantoolMemoryBytes,
      double activeRangeStreams) {}

  private record ScenarioResources(double peakHeapMiB, double maxActiveRangeStreams) {}

  private record StreamSaturationResult(
      boolean success, boolean rejected, long durationNanos, long itemCount) {}

  private record HeavyScaleReport(
      OffsetDateTime generatedAt,
      String command,
      String measurementMode,
      EnvironmentSnapshot environment,
      HeavyConfig config,
      SeedSummary seedSummary,
      List<ScenarioResult> results,
      List<PhaseSnapshot> phaseSnapshots,
      List<String> decisionRule,
      List<String> caveats) {}

  private record EnvironmentSnapshot(
      String osName,
      String osVersion,
      String osArch,
      String javaVersion,
      int availableProcessors,
      long maxHeapMegabytes) {

    static EnvironmentSnapshot capture() {
      long maxHeapBytes = Runtime.getRuntime().maxMemory();
      return new EnvironmentSnapshot(
          System.getProperty("os.name"),
          System.getProperty("os.version"),
          System.getProperty("os.arch"),
          System.getProperty("java.version"),
          Runtime.getRuntime().availableProcessors(),
          maxHeapBytes <= 0 ? -1 : maxHeapBytes / (1024 * 1024));
    }

    String describe() {
      return "%s %s (%s), Java %s, cpu=%d, maxHeapMiB=%d, jvm=%s"
          .formatted(
              this.osName,
              this.osVersion,
              this.osArch,
              this.javaVersion,
              this.availableProcessors,
              this.maxHeapMegabytes,
              ManagementFactory.getRuntimeMXBean().getVmName());
    }
  }

  private record HeavyConfig(
      long datasetSize,
      int valueBytes,
      long rangeWindowSize,
      long guardrailWindowSize,
      int guardrailRequestedStreams,
      int unaryConcurrency,
      int existingGetProbeCount,
      int unaryTempCrudProbeCount,
      int countMeasuredRuns,
      int rangeMeasuredRuns,
      int seedBatchSize,
      int seedConcurrency,
      Duration seedBatchTimeout,
      Duration seedOverallTimeout,
      Duration countClientDeadline,
      Duration rangeClientDeadline,
      Duration completionRequestTimeout) {

    static HeavyConfig fromSystemProperties() {
      HeavyConfig config =
          new HeavyConfig(
              Long.getLong("kvservice.perf-heavy.dataset-size", MAX_DATASET_SIZE),
              Integer.getInteger("kvservice.perf-heavy.value-bytes", 32),
              Long.getLong("kvservice.perf-heavy.range-window-size", 500_000L),
              Long.getLong("kvservice.perf-heavy.guardrail-window-size", 25_000L),
              Integer.getInteger("kvservice.perf-heavy.guardrail-requested-streams", 20),
              Integer.getInteger("kvservice.perf-heavy.unary-concurrency", 8),
              Integer.getInteger("kvservice.perf-heavy.existing-get-probes", 512),
              Integer.getInteger("kvservice.perf-heavy.temp-crud-probes", 128),
              Integer.getInteger("kvservice.perf-heavy.count-runs", 2),
              Integer.getInteger("kvservice.perf-heavy.range-runs", 2),
              Integer.getInteger("kvservice.perf-heavy.seed-batch-size", 20_000),
              Integer.getInteger("kvservice.perf-heavy.seed-concurrency", 4),
              Duration.ofSeconds(
                  Long.getLong("kvservice.perf-heavy.seed-batch-timeout-seconds", 120)),
              Duration.ofMinutes(
                  Long.getLong("kvservice.perf-heavy.seed-overall-timeout-minutes", 45)),
              Duration.ofSeconds(Long.getLong("kvservice.perf-heavy.count-deadline-seconds", 120)),
              Duration.ofSeconds(Long.getLong("kvservice.perf-heavy.range-deadline-seconds", 180)),
              Duration.ofSeconds(
                  Long.getLong("kvservice.perf-heavy.completion-request-timeout-seconds", 120)));
      assertThat(config.datasetSize()).isBetween(1L, MAX_DATASET_SIZE);
      assertThat(config.rangeWindowSize()).isBetween(1L, config.datasetSize());
      assertThat(config.guardrailWindowSize()).isBetween(1L, config.datasetSize());
      assertThat(config.valueBytes()).isGreaterThanOrEqualTo(9);
      assertThat(config.seedBatchSize()).isPositive();
      assertThat(config.seedConcurrency()).isPositive();
      assertThat(config.unaryConcurrency()).isPositive();
      assertThat(config.existingGetProbeCount()).isPositive();
      assertThat(config.unaryTempCrudProbeCount()).isPositive();
      assertThat(config.guardrailRequestedStreams()).isGreaterThan(16);
      return config;
    }
  }

  private record LatencySummary(
      int sampleCount, double p50Millis, double p95Millis, double p99Millis) {

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
          nanosToMillis(percentile(sorted, 0.99d)));
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

  private static final class ResourceMonitor implements AutoCloseable {

    private final ServiceRuntime runtime;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread samplerThread;
    private final AtomicLong peakHeapBytes = new AtomicLong();
    private final AtomicLong peakActiveRangeStreamsScaled = new AtomicLong();

    private ResourceMonitor(ServiceRuntime runtime) {
      this.runtime = runtime;
      sample();
      this.samplerThread =
          new Thread(
              () -> {
                while (this.running.get()) {
                  sample();
                  try {
                    Thread.sleep(20L);
                  } catch (InterruptedException interruption) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              },
              "perf-heavy-resource-monitor");
      this.samplerThread.setDaemon(true);
      this.samplerThread.start();
    }

    private void sample() {
      this.peakHeapBytes.accumulateAndGet(this.runtime.currentHeapUsedBytes(), Math::max);
      long activeScaled = Math.round(this.runtime.currentActiveRangeStreams() * 1_000.0d);
      this.peakActiveRangeStreamsScaled.accumulateAndGet(activeScaled, Math::max);
    }

    ScenarioResources snapshot() {
      sample();
      return new ScenarioResources(
          this.peakHeapBytes.get() / (1024.0d * 1024.0d),
          this.peakActiveRangeStreamsScaled.get() / 1_000.0d);
    }

    @Override
    public void close() {
      this.running.set(false);
      this.samplerThread.interrupt();
      try {
        this.samplerThread.join(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException interruption) {
        Thread.currentThread().interrupt();
      }
      sample();
    }
  }

  private static final class ServiceRuntime implements AutoCloseable {

    private final String label;
    private final ConfigurableApplicationContext context;
    private final ManagedChannel channel;
    private final KvServiceGrpc.KvServiceBlockingStub blockingStub;
    private final TarantoolBoxClient tarantoolClient;
    private final MeterRegistry meterRegistry;
    private final MemoryMXBean memoryMxBean;

    private ServiceRuntime(
        String label,
        ConfigurableApplicationContext context,
        ManagedChannel channel,
        KvServiceGrpc.KvServiceBlockingStub blockingStub,
        TarantoolBoxClient tarantoolClient,
        MeterRegistry meterRegistry) {
      this.label = label;
      this.context = context;
      this.channel = channel;
      this.blockingStub = blockingStub;
      this.tarantoolClient = tarantoolClient;
      this.meterRegistry = meterRegistry;
      this.memoryMxBean = ManagementFactory.getMemoryMXBean();
    }

    static ServiceRuntime start(String label, Map<String, String> overrides) {
      if (!TARANTOOL.isRunning()) {
        TARANTOOL.start();
      }
      ConfigurableApplicationContext context =
          new SpringApplicationBuilder(KvServiceApplication.class).run(baseArguments(overrides));
      int grpcPort = context.getBean(GrpcServerLifecycle.class).getPort();
      ManagedChannel channel =
          ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
      return new ServiceRuntime(
          label,
          context,
          channel,
          KvServiceGrpc.newBlockingStub(channel),
          context.getBean(TarantoolBoxClient.class),
          context.getBean(MeterRegistry.class));
    }

    private static String[] baseArguments(Map<String, String> overrides) {
      Map<String, String> properties = new java.util.LinkedHashMap<>();
      properties.put("server.port", "0");
      properties.put("spring.grpc.server.port", "0");
      properties.put("management.tracing.export.enabled", "false");
      properties.put("kvservice.tarantool.host", TARANTOOL.getHost());
      properties.put(
          "kvservice.tarantool.port", Integer.toString(TARANTOOL.getMappedPort(TARANTOOL_PORT)));
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
      return ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
    }

    void truncateSpace() throws Exception {
      this.tarantoolClient
          .eval(
              "if box.space."
                  + SPACE_NAME
                  + " ~= nil then box.space."
                  + SPACE_NAME
                  + ":truncate() end return {true}")
          .get(30, TimeUnit.SECONDS);
    }

    long currentHeapUsedBytes() {
      MemoryUsage heap = this.memoryMxBean.getHeapMemoryUsage();
      return heap == null ? -1L : heap.getUsed();
    }

    long currentNonHeapUsedBytes() {
      MemoryUsage nonHeap = this.memoryMxBean.getNonHeapMemoryUsage();
      return nonHeap == null ? -1L : nonHeap.getUsed();
    }

    double currentActiveRangeStreams() {
      var gauge = this.meterRegistry.find(RANGE_ACTIVE_METRIC).gauge();
      return gauge == null ? 0.0d : gauge.value();
    }

    PhaseSnapshot capturePhaseSnapshot(String phase) {
      return new PhaseSnapshot(
          phase,
          currentHeapUsedBytes(),
          currentNonHeapUsedBytes(),
          currentTarantoolMemoryBytes(),
          currentActiveRangeStreams());
    }

    private long currentTarantoolMemoryBytes() {
      try {
        org.testcontainers.containers.Container.ExecResult execResult =
            TARANTOOL.execInContainer(
                "sh",
                "-lc",
                "cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null");
        if (execResult.getExitCode() != 0) {
          return -1L;
        }
        return Long.parseLong(execResult.getStdout().trim());
      } catch (Exception exception) {
        return -1L;
      }
    }

    <T> List<T> runConcurrent(String name, int concurrency, WorkerFactory<T> workerFactory)
        throws Exception {
      ExecutorService executor = Executors.newFixedThreadPool(concurrency);
      CountDownLatch startGate = new CountDownLatch(1);
      try {
        List<Future<T>> futures = new ArrayList<>();
        for (int workerIndex = 0; workerIndex < concurrency; workerIndex++) {
          int capturedIndex = workerIndex;
          futures.add(
              executor.submit(
                  () -> {
                    assertThat(startGate.await(10, TimeUnit.SECONDS))
                        .as("start gate for %s", name)
                        .isTrue();
                    return workerFactory.create(capturedIndex).call();
                  }));
        }
        startGate.countDown();
        List<T> results = new ArrayList<>(concurrency);
        for (Future<T> future : futures) {
          results.add(future.get(30, TimeUnit.MINUTES));
        }
        return results;
      } finally {
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
