package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import io.kvservice.application.RequestBudget;
import io.kvservice.application.RequestDeadlineExceededException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GrpcUnaryRequestBudgetFactoryTest {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void shutdownScheduler() {
        this.scheduler.shutdownNow();
    }

    @Test
    void usesServerDefaultWhenClientDeadlineIsAbsent() {
        GrpcUnaryRequestBudgetFactory factory = new GrpcUnaryRequestBudgetFactory(Duration.ofSeconds(3));

        RequestBudget budget = factory.create();

        assertThat(budget.remainingTimeout()).isBetween(Duration.ofSeconds(2), Duration.ofSeconds(3));
    }

    @Test
    void respectsClientDeadlineWhenProvided() throws Exception {
        GrpcUnaryRequestBudgetFactory factory = new GrpcUnaryRequestBudgetFactory(Duration.ofSeconds(3));

        Duration remaining = Context.current()
                .withDeadlineAfter(50, TimeUnit.MILLISECONDS, this.scheduler)
                .call(() -> factory.create().remainingTimeout());

        assertThat(remaining).isPositive();
        assertThat(remaining).isLessThan(Duration.ofSeconds(1));
    }

    @Test
    void failsWhenClientDeadlineIsAlreadyExceeded() {
        GrpcUnaryRequestBudgetFactory factory = new GrpcUnaryRequestBudgetFactory(Duration.ofSeconds(3));

        assertThatThrownBy(() -> Context.current()
                .withDeadlineAfter(0, TimeUnit.MILLISECONDS, this.scheduler)
                .call(factory::create))
                .isInstanceOf(RequestDeadlineExceededException.class)
                .hasMessage("deadline exceeded");
    }
}
