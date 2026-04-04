package io.kvservice.application;

import java.util.Objects;

import io.kvservice.application.storage.KeyValueStoragePort;

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
