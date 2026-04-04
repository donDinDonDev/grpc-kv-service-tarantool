package io.kvservice.observability;

import java.util.UUID;

import io.grpc.Context;
import io.grpc.Metadata;

final class CorrelationIdSupport {

    static final Metadata.Key<String> REQUEST_ID_HEADER = Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER);

    static final Metadata.Key<String> CORRELATION_ID_HEADER =
            Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

    static final Context.Key<String> REQUEST_ID_CONTEXT = Context.key("kvservice.request-id");

    private CorrelationIdSupport() {
    }

    static String resolveRequestId(Metadata headers) {
        String propagatedRequestId = headers.get(REQUEST_ID_HEADER);
        if (propagatedRequestId != null && !propagatedRequestId.isBlank()) {
            return propagatedRequestId;
        }
        String propagatedCorrelationId = headers.get(CORRELATION_ID_HEADER);
        if (propagatedCorrelationId != null && !propagatedCorrelationId.isBlank()) {
            return propagatedCorrelationId;
        }
        return UUID.randomUUID().toString();
    }

    static String currentRequestId() {
        String requestId = REQUEST_ID_CONTEXT.get();
        return requestId != null ? requestId : "unknown";
    }
}
