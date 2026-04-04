package io.kvservice.transport.grpc;

import java.time.Duration;
import io.kvservice.application.RequestBudget;

public final class GrpcUnaryRequestBudgetFactory {

    private final Duration defaultUnaryTimeout;

    public GrpcUnaryRequestBudgetFactory(Duration defaultUnaryTimeout) {
        this.defaultUnaryTimeout = defaultUnaryTimeout;
    }

    public RequestBudget create() {
        return GrpcRequestBudgetSupport.create(this.defaultUnaryTimeout);
    }
}
