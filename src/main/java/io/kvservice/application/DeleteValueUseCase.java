package io.kvservice.application;

import io.kvservice.application.storage.KeyValueStoragePort;

public final class DeleteValueUseCase {

    private final KeyValueStoragePort storage;
    private final KeyValueValidator validator;

    public DeleteValueUseCase(KeyValueStoragePort storage, KeyValueValidator validator) {
        this.storage = storage;
        this.validator = validator;
    }

    public void execute(String key, RequestBudget requestBudget) {
        this.validator.validateKey(key);
        requestBudget.throwIfCancelled();
        this.storage.delete(key, requestBudget.remainingTimeout());
    }
}
