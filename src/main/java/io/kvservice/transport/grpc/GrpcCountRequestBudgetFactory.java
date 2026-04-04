package io.kvservice.transport.grpc;

import java.time.Duration;

import io.kvservice.application.RequestBudget;

public final class GrpcCountRequestBudgetFactory {

    private final Duration defaultCountTimeout;

    public GrpcCountRequestBudgetFactory(Duration defaultCountTimeout) {
        this.defaultCountTimeout = defaultCountTimeout;
    }

    public RequestBudget create() {
        return GrpcRequestBudgetSupport.create(this.defaultCountTimeout);
    }
}
