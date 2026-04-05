package io.kvservice.transport.grpc;

import com.google.protobuf.ByteString;
import io.kvservice.api.v1.KvRecord;
import io.kvservice.api.v1.NullMarker;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import java.util.Optional;

public final class GrpcNullableBytesMapper {

  public Optional<StoredValue> fromRequestValue(NullableBytes value, boolean fieldPresent) {
    if (!fieldPresent || value == null) {
      return Optional.empty();
    }
    return switch (value.getKindCase()) {
      case DATA -> Optional.of(StoredValue.bytes(value.getData().toByteArray()));
      case NULL_VALUE -> Optional.of(StoredValue.nullValue());
      case KIND_NOT_SET -> Optional.empty();
    };
  }

  public KvRecord toRecord(StoredEntry entry) {
    return KvRecord.newBuilder()
        .setKey(entry.key())
        .setValue(toNullableBytes(entry.value()))
        .build();
  }

  private NullableBytes toNullableBytes(StoredValue value) {
    NullableBytes.Builder builder = NullableBytes.newBuilder();
    if (value instanceof StoredValue.BytesValue bytesValue) {
      return builder.setData(ByteString.copyFrom(bytesValue.bytes())).build();
    }
    return builder.setNullValue(NullMarker.getDefaultInstance()).build();
  }
}
