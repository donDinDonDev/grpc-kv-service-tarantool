package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Context;
import io.kvservice.application.RequestBudget;
import io.kvservice.application.RequestDeadlineExceededException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GrpcCountRequestBudgetFactoryTest {

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  @AfterEach
  void shutdownScheduler() {
    this.scheduler.shutdownNow();
  }

  @Test
  void usesCountProfileWhenClientDeadlineIsAbsent() {
    GrpcCountRequestBudgetFactory factory =
        new GrpcCountRequestBudgetFactory(Duration.ofSeconds(15));

    RequestBudget budget = factory.create();

    assertThat(budget.remainingTimeout()).isBetween(Duration.ofSeconds(14), Duration.ofSeconds(15));
  }

  @Test
  void respectsClientDeadlineWhenProvided() throws Exception {
    GrpcCountRequestBudgetFactory factory =
        new GrpcCountRequestBudgetFactory(Duration.ofSeconds(15));

    Duration remaining =
        Context.current()
            .withDeadlineAfter(50, TimeUnit.MILLISECONDS, this.scheduler)
            .call(() -> factory.create().remainingTimeout());

    assertThat(remaining).isPositive();
    assertThat(remaining).isLessThan(Duration.ofSeconds(1));
  }

  @Test
  void failsWhenClientDeadlineIsAlreadyExceeded() {
    GrpcCountRequestBudgetFactory factory =
        new GrpcCountRequestBudgetFactory(Duration.ofSeconds(15));

    assertThatThrownBy(
            () ->
                Context.current()
                    .withDeadlineAfter(0, TimeUnit.MILLISECONDS, this.scheduler)
                    .call(factory::create))
        .isInstanceOf(RequestDeadlineExceededException.class)
        .hasMessage("deadline exceeded");
  }
}
