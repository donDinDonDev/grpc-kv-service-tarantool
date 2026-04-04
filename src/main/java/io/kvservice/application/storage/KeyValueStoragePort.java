package io.kvservice.application.storage;

import java.util.List;
import java.util.Optional;

public interface KeyValueStoragePort {

    void put(String key, StoredValue value);

    Optional<StoredEntry> get(String key);

    void delete(String key);

    long count();

    List<StoredEntry> getRangeBatch(RangeBatchQuery query);
}
