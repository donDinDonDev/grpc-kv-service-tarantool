package io.kvservice.transport.grpc;

import io.grpc.Context;
import io.grpc.Deadline;
import io.kvservice.application.RequestBudget;
import io.kvservice.application.RequestDeadlineExceededException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class GrpcRequestBudgetSupport {

  private GrpcRequestBudgetSupport() {}

  static RequestBudget create(Duration defaultTimeout) {
    Context context = Context.current();
    Deadline deadline = context.getDeadline();
    Duration timeout =
        deadline == null
            ? defaultTimeout
            : Duration.ofNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS));
    if (timeout.isNegative() || timeout.isZero()) {
      throw new RequestDeadlineExceededException("deadline exceeded");
    }
    return RequestBudget.of(
        timeout, () -> context.isCancelled() && !deadlineExpired(context, deadline));
  }

  private static boolean deadlineExpired(Context context, Deadline deadline) {
    return deadline != null && context.isCancelled() && deadline.isExpired();
  }
}
