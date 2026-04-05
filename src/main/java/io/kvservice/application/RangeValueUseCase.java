package io.kvservice.application;

import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.RangeBatchQuery;
import io.kvservice.application.storage.StoredEntry;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class RangeValueUseCase {

  private final KeyValueStoragePort storage;
  private final KeyValueValidator validator;
  private final int batchSize;

  public RangeValueUseCase(
      KeyValueStoragePort storage, KeyValueValidator validator, int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.storage = Objects.requireNonNull(storage, "storage");
    this.validator = Objects.requireNonNull(validator, "validator");
    this.batchSize = batchSize;
  }

  public void stream(
      String keySince,
      String keyTo,
      RequestBudget requestBudget,
      Consumer<StoredEntry> itemConsumer) {
    String keyFromInclusive = this.validator.validateKey(keySince);
    String keyToExclusive = this.validator.validateKey(keyTo);
    if (Utf8LexicographicKeyOrder.compare(keyFromInclusive, keyToExclusive) >= 0) {
      throw new InvalidArgumentException("key_since must be less than key_to");
    }
    Objects.requireNonNull(requestBudget, "requestBudget");
    Objects.requireNonNull(itemConsumer, "itemConsumer");

    String startAfter = null;
    while (true) {
      requestBudget.throwIfCancelled();
      List<StoredEntry> batch =
          this.storage.getRangeBatch(
              new RangeBatchQuery(keyFromInclusive, keyToExclusive, startAfter, this.batchSize),
              requestBudget.remainingTimeout());
      if (batch.isEmpty()) {
        return;
      }
      for (StoredEntry entry : batch) {
        requestBudget.throwIfCancelled();
        itemConsumer.accept(entry);
        startAfter = entry.key();
      }
      if (batch.size() < this.batchSize) {
        return;
      }
    }
  }
}
