package io.kvservice.application;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class RequestBudget {

  private final long deadlineNanos;
  private final BooleanSupplier cancelled;

  private RequestBudget(Duration timeout, BooleanSupplier cancelled) {
    if (timeout == null || timeout.isNegative() || timeout.isZero()) {
      throw new RequestDeadlineExceededException("deadline exceeded");
    }
    this.deadlineNanos = System.nanoTime() + timeout.toNanos();
    this.cancelled = Objects.requireNonNull(cancelled, "cancelled");
  }

  public static RequestBudget of(Duration timeout, BooleanSupplier cancelled) {
    return new RequestBudget(timeout, cancelled);
  }

  public Duration remainingTimeout() {
    throwIfCancelled();
    long remainingNanos = this.deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new RequestDeadlineExceededException("deadline exceeded");
    }
    return Duration.ofNanos(remainingNanos);
  }

  public void throwIfCancelled() {
    if (this.cancelled.getAsBoolean()) {
      throw new RequestCancelledException("request cancelled");
    }
  }
}
