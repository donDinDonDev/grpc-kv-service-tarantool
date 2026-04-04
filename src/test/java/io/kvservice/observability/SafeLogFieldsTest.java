package io.kvservice.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import org.junit.jupiter.api.Test;

class SafeLogFieldsTest {

    @Test
    void putRequestFieldsContainOnlyHashedKeyAndPayloadMetadata() {
        PutRequest request = PutRequest.newBuilder()
                .setKey("secret-key")
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("super-secret")).build())
                .build();

        assertThat(SafeLogFields.forRequest(request))
                .containsEntry("key.bytes", "10")
                .containsEntry("value.kind", "data")
                .containsEntry("value.bytes", "12")
                .containsKey("key.sha256")
                .doesNotContainValue("secret-key")
                .doesNotContainValue("super-secret");
    }
}
