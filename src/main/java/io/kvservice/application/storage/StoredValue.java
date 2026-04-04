package io.kvservice.application.storage;

import java.util.Arrays;

/**
 * Явно различает null-значение и бинарный payload.
 */
public sealed interface StoredValue permits StoredValue.BytesValue, StoredValue.NullValue {

    static StoredValue nullValue() {
        return NullValue.INSTANCE;
    }

    static StoredValue bytes(byte[] value) {
        return new BytesValue(value);
    }

    default boolean isNull() {
        return this instanceof NullValue;
    }

    record BytesValue(byte[] bytes) implements StoredValue {

        public BytesValue {
            bytes = Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public byte[] bytes() {
            return Arrays.copyOf(this.bytes, this.bytes.length);
        }
    }

    final class NullValue implements StoredValue {

        private static final NullValue INSTANCE = new NullValue();

        private NullValue() {
        }
    }
}
