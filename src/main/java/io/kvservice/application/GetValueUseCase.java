package io.kvservice.application;

import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.StoredEntry;

public final class GetValueUseCase {

  private final KeyValueStoragePort storage;
  private final KeyValueValidator validator;

  public GetValueUseCase(KeyValueStoragePort storage, KeyValueValidator validator) {
    this.storage = storage;
    this.validator = validator;
  }

  public StoredEntry execute(String key, RequestBudget requestBudget) {
    this.validator.validateKey(key);
    requestBudget.throwIfCancelled();
    return this.storage
        .get(key, requestBudget.remainingTimeout())
        .orElseThrow(() -> new RecordNotFoundException("key not found"));
  }
}
