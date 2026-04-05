package io.kvservice.storage.tarantool;

import io.kvservice.application.storage.StorageAccessException;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import io.tarantool.mapping.Tuple;
import java.nio.ByteBuffer;
import java.util.List;

final class TarantoolTupleMapper {

  StoredEntry toStoredEntry(Tuple<List<?>> tuple) {
    List<?> rawTuple = tuple.get();
    if (rawTuple == null || rawTuple.size() < 2) {
      throw StorageAccessException.internal("unexpected tuple shape returned by Tarantool");
    }

    Object rawKey = rawTuple.getFirst();
    if (!(rawKey instanceof String key) || key.isEmpty()) {
      throw StorageAccessException.internal("unexpected key returned by Tarantool");
    }

    return new StoredEntry(key, toStoredValue(rawTuple.get(1)));
  }

  private StoredValue toStoredValue(Object rawValue) {
    if (rawValue == null) {
      return StoredValue.nullValue();
    }
    if (rawValue instanceof byte[] bytes) {
      return StoredValue.bytes(bytes);
    }
    if (rawValue instanceof ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.duplicate().get(bytes);
      return StoredValue.bytes(bytes);
    }
    throw StorageAccessException.internal(
        "unexpected value type returned by Tarantool: " + rawValue.getClass().getName());
  }
}
