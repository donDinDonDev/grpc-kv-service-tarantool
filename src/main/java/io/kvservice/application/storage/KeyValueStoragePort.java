package io.kvservice.application.storage;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public interface KeyValueStoragePort {

  default void put(String key, StoredValue value) {
    put(key, value, null);
  }

  void put(String key, StoredValue value, Duration timeout);

  default Optional<StoredEntry> get(String key) {
    return get(key, null);
  }

  Optional<StoredEntry> get(String key, Duration timeout);

  default void delete(String key) {
    delete(key, null);
  }

  void delete(String key, Duration timeout);

  default long count() {
    return count(null);
  }

  long count(Duration timeout);

  default List<StoredEntry> getRangeBatch(RangeBatchQuery query) {
    return getRangeBatch(query, null);
  }

  List<StoredEntry> getRangeBatch(RangeBatchQuery query, Duration timeout);
}
