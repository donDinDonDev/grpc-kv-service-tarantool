package io.kvservice.application.storage;

public record StoredEntry(String key, StoredValue value) {

  public StoredEntry {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("key must not be empty");
    }
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
  }
}
