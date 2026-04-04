package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.NullMarker;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import org.junit.jupiter.api.Test;

class GrpcNullableBytesMapperTest {

    private final GrpcNullableBytesMapper mapper = new GrpcNullableBytesMapper();

    @Test
    void distinguishesAbsentNullAndEmptyBytesOnRequestMapping() {
        assertThat(this.mapper.fromRequestValue(null, false)).isEmpty();

        StoredValue nullValue = this.mapper.fromRequestValue(
                NullableBytes.newBuilder().setNullValue(NullMarker.getDefaultInstance()).build(),
                true
        ).orElseThrow();
        assertThat(nullValue.isNull()).isTrue();

        StoredValue emptyBytes = this.mapper.fromRequestValue(
                NullableBytes.newBuilder().setData(com.google.protobuf.ByteString.empty()).build(),
                true
        ).orElseThrow();
        assertThat(emptyBytes).isInstanceOf(StoredValue.BytesValue.class);
        assertThat(((StoredValue.BytesValue) emptyBytes).bytes()).isEmpty();
    }

    @Test
    void distinguishesNullAndEmptyBytesOnResponseMapping() {
        assertThat(this.mapper.toRecord(new StoredEntry("null-key", StoredValue.nullValue())).getValue().getKindCase())
                .isEqualTo(NullableBytes.KindCase.NULL_VALUE);

        assertThat(this.mapper.toRecord(new StoredEntry("empty-key", StoredValue.bytes(new byte[0]))).getValue().getKindCase())
                .isEqualTo(NullableBytes.KindCase.DATA);
        assertThat(this.mapper.toRecord(new StoredEntry("empty-key", StoredValue.bytes(new byte[0]))).getValue().getData())
                .isEmpty();
    }
}
