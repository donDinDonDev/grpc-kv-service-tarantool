package io.kvservice.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Context;
import io.kvservice.application.storage.StorageAccessException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class TarantoolObservabilityTest {

  @Test
  void recordsTimeoutAndReconnectSignalsWithoutLeakingRawKey(CapturedOutput output) {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    TarantoolObservability observability =
        new TarantoolObservability(meterRegistry, ObservationRegistry.create(), Tracer.NOOP);

    Context.current()
        .withValue(CorrelationIdSupport.REQUEST_ID_CONTEXT, "req-789")
        .run(
            () ->
                assertThatThrownBy(
                        () ->
                            observability.observe(
                                "get",
                                SafeLogFields.forKey("key", "secret-key"),
                                () -> {
                                  throw StorageAccessException.deadlineExceeded(
                                      "Tarantool request timed out",
                                      new TimeoutException("secret-key"));
                                }))
                    .isInstanceOf(StorageAccessException.class));

    observability.onReconnectScheduled("127.0.0.1", 3301, 250);

    assertThat(
            meterRegistry
                .get("kvservice.tarantool.operation.count")
                .tag("operation", "get")
                .tag("outcome", "deadline_exceeded")
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(
            meterRegistry
                .get("kvservice.tarantool.connection.event.count")
                .tag("event", "reconnect_scheduled")
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(output.getOut() + output.getErr())
        .contains("\"event\":\"Tarantool operation failed\"")
        .contains("\"request.id\":\"req-789\"")
        .contains("\"tarantool.operation\":\"get\"")
        .contains("\"tarantool.outcome\":\"deadline_exceeded\"")
        .contains("\"key.bytes\":\"10\"")
        .contains("\"key.sha256\":\"85dbe15d75ef9308\"")
        .contains("\"event\":\"Tarantool reconnect scheduled\"")
        .contains("\"tarantool.reconnect_after_ms\":\"250\"")
        .doesNotContain("secret-key");
  }
}
