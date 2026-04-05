package io.kvservice.transport.grpc;

import io.kvservice.application.RequestBudget;
import java.time.Duration;

public final class GrpcUnaryRequestBudgetFactory {

  private final Duration defaultUnaryTimeout;

  public GrpcUnaryRequestBudgetFactory(Duration defaultUnaryTimeout) {
    this.defaultUnaryTimeout = defaultUnaryTimeout;
  }

  public RequestBudget create() {
    return GrpcRequestBudgetSupport.create(this.defaultUnaryTimeout);
  }
}
