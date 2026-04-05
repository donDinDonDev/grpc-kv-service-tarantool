package io.kvservice.observability;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.grpc.Context;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.Contexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcObservabilityInterceptor implements ServerInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcObservabilityInterceptor.class);
    private static final Metadata.Key<String> TRACEPARENT_HEADER = Metadata.Key.of("traceparent",
            Metadata.ASCII_STRING_MARSHALLER);
    private static final Propagator.Getter<Metadata> GRPC_METADATA_GETTER = new Propagator.Getter<>() {
        @Override
        public String get(Metadata carrier, String key) {
            if (carrier == null || key == null) {
                return null;
            }
            return carrier.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
        }
    };

    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final Propagator propagator;

    public GrpcObservabilityInterceptor(MeterRegistry meterRegistry, Tracer tracer, Propagator propagator) {
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        String requestId = CorrelationIdSupport.resolveRequestId(headers);
        String method = call.getMethodDescriptor().getBareMethodName();
        String service = call.getMethodDescriptor().getServiceName();
        long startedAtNanos = System.nanoTime();
        AtomicReference<Object> requestRef = new AtomicReference<>();
        Span requestSpan = startServerSpan(headers, service, method);

        Map<String, String> baseFields = new LinkedHashMap<>();
        baseFields.put("request.id", requestId);
        baseFields.put("grpc.service", service);
        baseFields.put("grpc.method", method);

        ServerCall<ReqT, RespT> forwardingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void sendHeaders(Metadata responseHeaders) {
                responseHeaders.put(CorrelationIdSupport.REQUEST_ID_HEADER, requestId);
                super.sendHeaders(responseHeaders);
            }

            @Override
            public void close(Status status, Metadata trailers) {
                try {
                    withBaseFieldsAndTrace(baseFields, requestSpan, () -> {
                        trailers.put(CorrelationIdSupport.REQUEST_ID_HEADER, requestId);
                        tagSpan(requestSpan, status);
                        recordRequestMetrics(service, method, status, System.nanoTime() - startedAtNanos);
                        if (!status.isOk()) {
                            logFailure(status, baseFields, requestRef.get());
                        }
                        super.close(status, trailers);
                    });
                }
                finally {
                    requestSpan.end();
                }
            }
        };

        Context context = Context.current().withValue(CorrelationIdSupport.REQUEST_ID_CONTEXT, requestId);
        ServerCall.Listener<ReqT> delegate = withBaseFieldsAndTrace(baseFields, requestSpan,
                () -> Contexts.interceptCall(context, forwardingCall, headers, next));
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onMessage(ReqT message) {
                requestRef.compareAndSet(null, message);
                withBaseFieldsAndTrace(baseFields, requestSpan, () -> super.onMessage(message));
            }

            @Override
            public void onHalfClose() {
                withBaseFieldsAndTrace(baseFields, requestSpan, super::onHalfClose);
            }

            @Override
            public void onCancel() {
                withBaseFieldsAndTrace(baseFields, requestSpan, super::onCancel);
            }

            @Override
            public void onComplete() {
                withBaseFieldsAndTrace(baseFields, requestSpan, super::onComplete);
            }

            @Override
            public void onReady() {
                withBaseFieldsAndTrace(baseFields, requestSpan, super::onReady);
            }
        };
    }

    private void logFailure(Status status, Map<String, String> baseFields, Object request) {
        Map<String, String> fields = new LinkedHashMap<>(baseFields);
        fields.put("grpc.status", status.getCode().name());
        if (status.getDescription() != null) {
            fields.put("grpc.description", status.getDescription());
        }
        fields.putAll(SafeLogFields.forRequest(request));
        TraceLogFields.put(fields, this.tracer.currentSpan());
        try (MdcScope ignored = MdcScope.open(fields)) {
            if (status.getCode() == Status.Code.CANCELLED) {
                LOGGER.info(StructuredLogMessage.of("gRPC request cancelled", fields));
            }
            else {
                LOGGER.warn(StructuredLogMessage.of("gRPC request failed", fields));
            }
        }
    }

    private void recordRequestMetrics(String service, String method, Status status, long durationNanos) {
        Counter.builder("kvservice.grpc.server.request.count")
                .tag("service", service)
                .tag("method", method)
                .register(this.meterRegistry)
                .increment();
        Timer.builder("kvservice.grpc.server.request.latency")
                .tag("service", service)
                .tag("method", method)
                .tag("status", status.getCode().name())
                .register(this.meterRegistry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
        if (!status.isOk()) {
            Counter.builder("kvservice.grpc.server.error.count")
                    .tag("service", service)
                    .tag("method", method)
                    .tag("status", status.getCode().name())
                    .register(this.meterRegistry)
                    .increment();
        }
    }

    private Span startServerSpan(Metadata headers, String service, String method) {
        Span.Builder spanBuilder = this.tracer.spanBuilder();
        TraceContext parentTraceContext = traceContextFromTraceparent(headers);
        if (parentTraceContext != null) {
            spanBuilder = spanBuilder.setParent(parentTraceContext);
        }
        else {
            spanBuilder = this.propagator.extract(headers, GRPC_METADATA_GETTER);
        }
        return spanBuilder.kind(Span.Kind.SERVER)
                .name(service + "/" + method)
                .tag("rpc.system", "grpc")
                .tag("rpc.service", service)
                .tag("rpc.method", method)
                .start();
    }

    private void tagSpan(Span requestSpan, Status status) {
        requestSpan.tag("grpc.status", status.getCode().name());
        if (!status.isOk()) {
            requestSpan.event("grpc." + status.getCode().name().toLowerCase());
        }
    }

    private <T> T withBaseFieldsAndTrace(Map<String, String> baseFields, Span requestSpan, Supplier<T> action) {
        try (MdcScope ignored = MdcScope.open(fieldsWithTrace(baseFields, requestSpan));
                TraceScope traceScope = TraceScope.open(this.tracer, requestSpan)) {
            return action.get();
        }
    }

    private void withBaseFieldsAndTrace(Map<String, String> baseFields, Span requestSpan, Runnable action) {
        try (MdcScope ignored = MdcScope.open(fieldsWithTrace(baseFields, requestSpan));
                TraceScope traceScope = TraceScope.open(this.tracer, requestSpan)) {
            action.run();
        }
    }

    private Map<String, String> fieldsWithTrace(Map<String, String> baseFields, Span requestSpan) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>(baseFields);
        TraceLogFields.put(fields, requestSpan);
        return Map.copyOf(fields);
    }

    private TraceContext traceContextFromTraceparent(Metadata headers) {
        String traceparent = headers.get(TRACEPARENT_HEADER);
        if (traceparent == null || traceparent.isBlank()) {
            return null;
        }
        String[] parts = traceparent.trim().split("-");
        if (parts.length != 4 || parts[1].length() != 32 || parts[2].length() != 16 || parts[3].length() != 2) {
            return null;
        }
        if (!isLowercaseHex(parts[1]) || !isLowercaseHex(parts[2]) || !isLowercaseHex(parts[3])) {
            return null;
        }
        boolean sampled = (Integer.parseInt(parts[3], 16) & 1) == 1;
        return this.tracer.traceContextBuilder()
                .traceId(parts[1])
                .parentId(parts[2])
                .spanId(parts[2])
                .sampled(sampled)
                .build();
    }

    private boolean isLowercaseHex(String value) {
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (!((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f'))) {
                return false;
            }
        }
        return true;
    }

    private static final class TraceScope implements AutoCloseable {

        private static final TraceScope NOOP = new TraceScope(null);

        private final Tracer.SpanInScope delegate;

        private TraceScope(Tracer.SpanInScope delegate) {
            this.delegate = delegate;
        }

        static TraceScope open(Tracer tracer, Span span) {
            if (span == null) {
                return NOOP;
            }
            return new TraceScope(tracer.withSpan(span));
        }

        @Override
        public void close() {
            if (this.delegate != null) {
                this.delegate.close();
            }
        }
    }
}
