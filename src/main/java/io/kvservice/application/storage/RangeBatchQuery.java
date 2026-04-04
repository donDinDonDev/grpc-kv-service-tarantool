package io.kvservice.application.storage;

public record RangeBatchQuery(
        String keyFromInclusive,
        String keyToExclusive,
        String startAfter,
        int limit
) {

    public RangeBatchQuery {
        if (keyFromInclusive == null || keyFromInclusive.isEmpty()) {
            throw new IllegalArgumentException("keyFromInclusive must not be empty");
        }
        if (keyToExclusive == null || keyToExclusive.isEmpty()) {
            throw new IllegalArgumentException("keyToExclusive must not be empty");
        }
        if (keyFromInclusive.compareTo(keyToExclusive) >= 0) {
            throw new IllegalArgumentException("keyFromInclusive must be less than keyToExclusive");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
    }
}
