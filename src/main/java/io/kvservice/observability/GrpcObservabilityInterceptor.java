package io.kvservice.observability;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcObservabilityInterceptor implements ServerInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcObservabilityInterceptor.class);

    private final MeterRegistry meterRegistry;

    public GrpcObservabilityInterceptor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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
                trailers.put(CorrelationIdSupport.REQUEST_ID_HEADER, requestId);
                recordRequestMetrics(service, method, status, System.nanoTime() - startedAtNanos);
                if (!status.isOk()) {
                    logFailure(status, baseFields, requestRef.get());
                }
                super.close(status, trailers);
            }
        };

        Context context = Context.current().withValue(CorrelationIdSupport.REQUEST_ID_CONTEXT, requestId);
        ServerCall.Listener<ReqT> delegate = Contexts.interceptCall(context, forwardingCall, headers, next);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onMessage(ReqT message) {
                requestRef.compareAndSet(null, message);
                withBaseFields(baseFields, () -> super.onMessage(message));
            }

            @Override
            public void onHalfClose() {
                withBaseFields(baseFields, super::onHalfClose);
            }

            @Override
            public void onCancel() {
                withBaseFields(baseFields, super::onCancel);
            }

            @Override
            public void onComplete() {
                withBaseFields(baseFields, super::onComplete);
            }

            @Override
            public void onReady() {
                withBaseFields(baseFields, super::onReady);
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

    private void withBaseFields(Map<String, String> baseFields, Runnable action) {
        try (MdcScope ignored = MdcScope.open(baseFields)) {
            action.run();
        }
    }
}
