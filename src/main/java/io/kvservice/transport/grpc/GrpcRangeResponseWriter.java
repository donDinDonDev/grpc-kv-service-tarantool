package io.kvservice.transport.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.kvservice.api.v1.RangeItem;
import io.kvservice.application.RequestBudget;
import io.kvservice.application.RequestCancelledException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class GrpcRangeResponseWriter {

  private static final long MAX_WAIT_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

  private final ServerCallStreamObserver<RangeItem> responseObserver;
  private final RequestBudget requestBudget;
  private final Object readinessMonitor = new Object();

  GrpcRangeResponseWriter(
      ServerCallStreamObserver<RangeItem> responseObserver, RequestBudget requestBudget) {
    this.responseObserver = Objects.requireNonNull(responseObserver, "responseObserver");
    this.requestBudget = Objects.requireNonNull(requestBudget, "requestBudget");
    this.responseObserver.setOnReadyHandler(this::signalReadiness);
    this.responseObserver.setOnCancelHandler(this::signalReadiness);
  }

  void write(RangeItem item) {
    Objects.requireNonNull(item, "item");
    awaitReady();
    this.requestBudget.throwIfCancelled();
    if (this.responseObserver.isCancelled()) {
      throw new RequestCancelledException("request cancelled");
    }
    this.responseObserver.onNext(item);
  }

  private void awaitReady() {
    while (!this.responseObserver.isReady()) {
      this.requestBudget.throwIfCancelled();
      if (this.responseObserver.isCancelled()) {
        throw new RequestCancelledException("request cancelled");
      }
      long waitNanos =
          Math.min(this.requestBudget.remainingTimeout().toNanos(), MAX_WAIT_SLICE_NANOS);
      long millis = TimeUnit.NANOSECONDS.toMillis(waitNanos);
      int nanos = (int) (waitNanos - TimeUnit.MILLISECONDS.toNanos(millis));
      synchronized (this.readinessMonitor) {
        if (this.responseObserver.isReady()) {
          return;
        }
        try {
          this.readinessMonitor.wait(millis, nanos);
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          throw new RequestCancelledException("request cancelled");
        }
      }
    }
  }

  private void signalReadiness() {
    synchronized (this.readinessMonitor) {
      this.readinessMonitor.notifyAll();
    }
  }
}
