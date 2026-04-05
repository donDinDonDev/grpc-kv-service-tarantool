package io.kvservice.transport.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

final class TestServerCallStreamObserver<T> extends ServerCallStreamObserver<T> {

  private final List<T> values = new CopyOnWriteArrayList<>();
  private volatile boolean ready = true;
  private volatile boolean cancelled;
  private volatile boolean completed;
  private volatile Throwable error;
  private volatile Runnable onReadyHandler = () -> {};
  private volatile Runnable onCancelHandler = () -> {};
  private volatile Runnable onCloseHandler = () -> {};

  List<T> values() {
    return this.values;
  }

  Throwable error() {
    return this.error;
  }

  boolean completed() {
    return this.completed;
  }

  void setReady(boolean ready) {
    this.ready = ready;
    if (ready) {
      this.onReadyHandler.run();
    }
  }

  void cancel() {
    this.cancelled = true;
    this.onCancelHandler.run();
    this.onCloseHandler.run();
  }

  @Override
  public boolean isCancelled() {
    return this.cancelled;
  }

  @Override
  public void setOnCancelHandler(Runnable onCancelHandler) {
    this.onCancelHandler = onCancelHandler == null ? () -> {} : onCancelHandler;
  }

  @Override
  public void setCompression(String compression) {}

  @Override
  public boolean isReady() {
    return this.ready;
  }

  @Override
  public void setOnReadyHandler(Runnable onReadyHandler) {
    this.onReadyHandler = onReadyHandler == null ? () -> {} : onReadyHandler;
  }

  @Override
  public void request(int count) {}

  @Override
  public void setMessageCompression(boolean enable) {}

  @Override
  public void disableAutoInboundFlowControl() {}

  @Override
  public void setOnCloseHandler(Runnable onCloseHandler) {
    this.onCloseHandler = onCloseHandler == null ? () -> {} : onCloseHandler;
  }

  @Override
  public void onNext(T value) {
    this.values.add(value);
  }

  @Override
  public void onError(Throwable t) {
    this.error = t;
    this.onCloseHandler.run();
  }

  @Override
  public void onCompleted() {
    this.completed = true;
    this.onCloseHandler.run();
  }
}
