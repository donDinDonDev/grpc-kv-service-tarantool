package io.kvservice.transport.grpc;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import io.grpc.Deadline;
import io.kvservice.application.RequestBudget;
import io.kvservice.application.RequestDeadlineExceededException;

public final class GrpcUnaryRequestBudgetFactory {

    private final Duration defaultUnaryTimeout;

    public GrpcUnaryRequestBudgetFactory(Duration defaultUnaryTimeout) {
        this.defaultUnaryTimeout = defaultUnaryTimeout;
    }

    public RequestBudget create() {
        Context context = Context.current();
        Deadline deadline = context.getDeadline();
        Duration timeout = deadline == null ? this.defaultUnaryTimeout : Duration.ofNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS));
        if (timeout.isNegative() || timeout.isZero()) {
            throw new RequestDeadlineExceededException("deadline exceeded");
        }
        return RequestBudget.of(timeout, context::isCancelled);
    }
}
