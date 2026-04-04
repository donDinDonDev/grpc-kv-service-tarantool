package io.kvservice.application;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import io.kvservice.application.storage.StoredValue;
import org.springframework.util.unit.DataSize;

public final class KeyValueValidator {

    private final long maxKeyBytes;
    private final long maxValueBytes;

    public KeyValueValidator(DataSize maxKeyBytes, DataSize maxValueBytes) {
        this.maxKeyBytes = maxKeyBytes.toBytes();
        this.maxValueBytes = maxValueBytes.toBytes();
    }

    public String validateKey(String key) {
        if (key == null) {
            throw new InvalidArgumentException("key is required");
        }
        if (key.isEmpty()) {
            throw new InvalidArgumentException("key must not be empty");
        }
        if (key.codePoints().anyMatch(Character::isISOControl)) {
            throw new InvalidArgumentException("key must not contain control characters");
        }
        int encodedLength = key.getBytes(StandardCharsets.UTF_8).length;
        if (encodedLength > this.maxKeyBytes) {
            throw new InvalidArgumentException("key exceeds max size");
        }
        return key;
    }

    public StoredValue requireExplicitValue(Optional<StoredValue> value) {
        if (value == null || value.isEmpty()) {
            throw new InvalidArgumentException("value is required");
        }
        return validateValue(value.orElseThrow());
    }

    public StoredValue validateValue(StoredValue value) {
        if (value == null) {
            throw new InvalidArgumentException("value is required");
        }
        if (value instanceof StoredValue.BytesValue bytesValue && bytesValue.bytes().length > this.maxValueBytes) {
            throw new InvalidArgumentException("value exceeds max size");
        }
        return value;
    }
}
