package io.kvservice.application;

import io.kvservice.application.storage.KeyValueStoragePort;
import java.util.Objects;

public final class CountEntriesUseCase {

  private final KeyValueStoragePort storage;

  public CountEntriesUseCase(KeyValueStoragePort storage) {
    this.storage = Objects.requireNonNull(storage, "storage");
  }

  public long execute(RequestBudget requestBudget) {
    Objects.requireNonNull(requestBudget, "requestBudget");
    requestBudget.throwIfCancelled();
    return this.storage.count(requestBudget.remainingTimeout());
  }
}
