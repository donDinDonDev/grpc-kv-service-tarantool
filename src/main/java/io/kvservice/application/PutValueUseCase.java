package io.kvservice.application;

import java.util.Optional;

import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.StoredValue;

public final class PutValueUseCase {

    private final KeyValueStoragePort storage;
    private final KeyValueValidator validator;

    public PutValueUseCase(KeyValueStoragePort storage, KeyValueValidator validator) {
        this.storage = storage;
        this.validator = validator;
    }

    public void execute(String key, Optional<StoredValue> value, RequestBudget requestBudget) {
        this.validator.validateKey(key);
        StoredValue explicitValue = this.validator.requireExplicitValue(value);
        requestBudget.throwIfCancelled();
        this.storage.put(key, explicitValue, requestBudget.remainingTimeout());
    }
}
