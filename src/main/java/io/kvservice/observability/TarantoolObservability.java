package io.kvservice.observability;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.kvservice.application.FailureCode;
import io.kvservice.application.KvServiceException;
import io.tarantool.pool.HeartbeatEvent;
import io.tarantool.pool.PoolEventListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TarantoolObservability implements PoolEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TarantoolObservability.class);

    private final MeterRegistry meterRegistry;
    private final ObservationRegistry observationRegistry;
    private final Tracer tracer;

    public TarantoolObservability(MeterRegistry meterRegistry, ObservationRegistry observationRegistry, Tracer tracer) {
        this.meterRegistry = meterRegistry;
        this.observationRegistry = observationRegistry;
        this.tracer = tracer;
    }

    public <T> T observe(String operation, Map<String, String> safeFields, Supplier<T> action) {
        long startedAtNanos = System.nanoTime();
        Observation observation = Observation.createNotStarted("kvservice.tarantool.operation", this.observationRegistry)
                .contextualName("tarantool " + operation)
                .lowCardinalityKeyValue("db.system", "tarantool")
                .lowCardinalityKeyValue("tarantool.operation", operation)
                .start();
        try {
            T result;
            try (Observation.Scope ignored = observation.openScope()) {
                result = action.get();
            }
            observation.lowCardinalityKeyValue("tarantool.outcome", "success");
            recordOperation(operation, "success", startedAtNanos);
            return result;
        }
        catch (RuntimeException failure) {
            String outcome = classifyOutcome(failure);
            observation.error(failure);
            observation.lowCardinalityKeyValue("tarantool.outcome", outcome);
            recordOperation(operation, outcome, startedAtNanos);
            logFailure(operation, outcome, safeFields, failure);
            throw failure;
        }
        finally {
            observation.stop();
        }
    }

    @Override
    public void onConnectionOpened(String host, int port) {
        recordConnectionEvent("opened");
    }

    @Override
    public void onConnectionClosed(String host, int port) {
        recordConnectionEvent("closed");
    }

    @Override
    public void onConnectionFailed(String host, int port, Throwable failure) {
        recordConnectionEvent("failed");
        try (MdcScope ignored = MdcScope.open(connectionFields(host, port, Map.of("tarantool.event", "connection_failed",
                "failure.type", failure.getClass().getSimpleName())))) {
            LOGGER.warn(StructuredLogMessage.of("Tarantool connection failed", connectionFields(host, port,
                    Map.of("tarantool.event", "connection_failed", "failure.type", failure.getClass().getSimpleName()))));
        }
    }

    @Override
    public void onReconnectScheduled(String host, int port, long reconnectAfterMillis) {
        recordConnectionEvent("reconnect_scheduled");
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("tarantool.event", "reconnect_scheduled");
        fields.put("tarantool.reconnect_after_ms", Long.toString(reconnectAfterMillis));
        try (MdcScope ignored = MdcScope.open(connectionFields(host, port, fields))) {
            LOGGER.warn(StructuredLogMessage.of("Tarantool reconnect scheduled", connectionFields(host, port, fields)));
        }
    }

    @Override
    public void onHeartbeatEvent(String host, int port, HeartbeatEvent heartbeatEvent) {
        recordConnectionEvent("heartbeat_" + heartbeatEvent.name().toLowerCase());
    }

    private void logFailure(String operation, String outcome, Map<String, String> safeFields, RuntimeException failure) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("request.id", CorrelationIdSupport.currentRequestId());
        fields.put("tarantool.operation", operation);
        fields.put("tarantool.outcome", outcome);
        fields.put("failure.type", failure.getClass().getSimpleName());
        if (safeFields != null) {
            fields.putAll(safeFields);
        }
        TraceLogFields.put(fields, this.tracer.currentSpan());
        try (MdcScope ignored = MdcScope.open(fields)) {
            if ("cancelled".equals(outcome)) {
                LOGGER.info(StructuredLogMessage.of("Tarantool operation cancelled", fields));
            }
            else {
                LOGGER.warn(StructuredLogMessage.of("Tarantool operation failed", fields));
            }
        }
    }

    private void recordOperation(String operation, String outcome, long startedAtNanos) {
        Counter.builder("kvservice.tarantool.operation.count")
                .tag("operation", operation)
                .tag("outcome", outcome)
                .register(this.meterRegistry)
                .increment();
        Timer.builder("kvservice.tarantool.operation.latency")
                .tag("operation", operation)
                .tag("outcome", outcome)
                .register(this.meterRegistry)
                .record(System.nanoTime() - startedAtNanos, TimeUnit.NANOSECONDS);
    }

    private void recordConnectionEvent(String event) {
        Counter.builder("kvservice.tarantool.connection.event.count")
                .tag("event", event)
                .register(this.meterRegistry)
                .increment();
    }

    private String classifyOutcome(RuntimeException failure) {
        if (failure instanceof KvServiceException serviceException) {
            return switch (serviceException.failureCode()) {
                case DEADLINE_EXCEEDED -> "deadline_exceeded";
                case CANCELLED -> "cancelled";
                case UNAVAILABLE -> "unavailable";
                case INVALID_ARGUMENT, NOT_FOUND, INTERNAL -> "internal";
            };
        }
        return "internal";
    }

    private Map<String, String> connectionFields(String host, int port, Map<String, String> extraFields) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>(extraFields);
        fields.put("tarantool.host", host);
        fields.put("tarantool.port", Integer.toString(port));
        return Map.copyOf(fields);
    }
}
