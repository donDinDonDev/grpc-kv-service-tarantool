package io.kvservice.observability;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

import io.kvservice.api.v1.DeleteRequest;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import io.kvservice.api.v1.RangeRequest;
import io.kvservice.application.storage.StoredValue;

public final class SafeLogFields {

    private static final int HASH_HEX_LENGTH = 16;

    private SafeLogFields() {
    }

    public static Map<String, String> forRequest(Object request) {
        if (request instanceof PutRequest putRequest) {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.putAll(forKey("key", putRequest.getKey()));
            if (!putRequest.hasValue()) {
                fields.put("value.kind", "unset");
                fields.put("value.bytes", "0");
                return Map.copyOf(fields);
            }
            NullableBytes value = putRequest.getValue();
            fields.put("value.kind", switch (value.getKindCase()) {
                case NULL_VALUE -> "null";
                case DATA -> "data";
                case KIND_NOT_SET -> "unset";
            });
            fields.put("value.bytes", Integer.toString(value.getKindCase() == NullableBytes.KindCase.DATA
                    ? value.getData().size()
                    : 0));
            return Map.copyOf(fields);
        }
        if (request instanceof GetRequest getRequest) {
            return forKey("key", getRequest.getKey());
        }
        if (request instanceof DeleteRequest deleteRequest) {
            return forKey("key", deleteRequest.getKey());
        }
        if (request instanceof RangeRequest rangeRequest) {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.putAll(forKey("range.key_since", rangeRequest.getKeySince()));
            fields.putAll(forKey("range.key_to", rangeRequest.getKeyTo()));
            return Map.copyOf(fields);
        }
        return Map.of();
    }

    public static Map<String, String> forPut(String key, StoredValue value) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.putAll(forKey("key", key));
        fields.put("value.kind", value.isNull() ? "null" : "data");
        fields.put("value.bytes", Integer.toString(value.isNull() ? 0 : ((StoredValue.BytesValue) value).bytes().length));
        return Map.copyOf(fields);
    }

    public static Map<String, String> forKey(String fieldPrefix, String key) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        if (key == null) {
            fields.put(fieldPrefix + ".bytes", "0");
            fields.put(fieldPrefix + ".sha256", "null");
            return Map.copyOf(fields);
        }
        fields.put(fieldPrefix + ".bytes", Integer.toString(key.getBytes(StandardCharsets.UTF_8).length));
        fields.put(fieldPrefix + ".sha256", hash(key));
        return Map.copyOf(fields);
    }

    public static Map<String, String> forRange(String keySince, String keyTo) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.putAll(forKey("range.key_since", keySince));
        fields.putAll(forKey("range.key_to", keyTo));
        return Map.copyOf(fields);
    }

    private static String hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(HASH_HEX_LENGTH);
            for (int index = 0; index < digest.length && result.length() < HASH_HEX_LENGTH; index++) {
                result.append(Character.forDigit((digest[index] >> 4) & 0xF, 16));
                result.append(Character.forDigit(digest[index] & 0xF, 16));
            }
            return result.substring(0, HASH_HEX_LENGTH);
        }
        catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}
