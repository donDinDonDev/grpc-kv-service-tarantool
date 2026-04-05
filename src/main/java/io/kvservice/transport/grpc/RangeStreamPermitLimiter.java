package io.kvservice.transport.grpc;

import io.kvservice.application.ServiceUnavailableException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RangeStreamPermitLimiter {

  private final Semaphore permits;

  public RangeStreamPermitLimiter(int maxActiveStreams) {
    if (maxActiveStreams <= 0) {
      throw new IllegalArgumentException("maxActiveStreams must be positive");
    }
    this.permits = new Semaphore(maxActiveStreams, true);
  }

  Permit acquire() {
    if (!this.permits.tryAcquire()) {
      throw new ServiceUnavailableException("too many active range streams");
    }
    return new Permit(this.permits);
  }

  static final class Permit implements AutoCloseable {

    private final Semaphore permits;
    private final AtomicBoolean released = new AtomicBoolean();

    private Permit(Semaphore permits) {
      this.permits = permits;
    }

    @Override
    public void close() {
      if (this.released.compareAndSet(false, true)) {
        this.permits.release();
      }
    }
  }
}
