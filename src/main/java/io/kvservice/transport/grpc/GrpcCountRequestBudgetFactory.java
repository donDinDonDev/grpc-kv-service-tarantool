package io.kvservice.transport.grpc;

import io.kvservice.application.RequestBudget;
import java.time.Duration;

public final class GrpcCountRequestBudgetFactory {

  private final Duration defaultCountTimeout;

  public GrpcCountRequestBudgetFactory(Duration defaultCountTimeout) {
    this.defaultCountTimeout = defaultCountTimeout;
  }

  public RequestBudget create() {
    return GrpcRequestBudgetSupport.create(this.defaultCountTimeout);
  }
}
