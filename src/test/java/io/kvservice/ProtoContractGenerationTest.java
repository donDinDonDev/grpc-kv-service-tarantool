package io.kvservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.MethodDescriptor;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.NullableBytes;
import io.kvservice.api.v1.PutRequest;
import org.junit.jupiter.api.Test;

class ProtoContractGenerationTest {

  @Test
  void generatedClassesUseExpectedJavaPackage() {
    assertThat(KvServiceGrpc.class.getPackageName()).isEqualTo("io.kvservice.api.v1");
    assertThat(PutRequest.class.getPackageName()).isEqualTo("io.kvservice.api.v1");
    assertThat(NullableBytes.class.getPackageName()).isEqualTo("io.kvservice.api.v1");
  }

  @Test
  void generatedServiceDescriptorMatchesPublicContract() {
    assertThat(KvServiceGrpc.getServiceDescriptor().getName()).isEqualTo("kv.v1.KvService");
    assertThat(KvServiceGrpc.getServiceDescriptor().getMethods())
        .extracting(MethodDescriptor::getBareMethodName)
        .containsExactly("Put", "Get", "Delete", "Range", "Count");

    assertThat(NullableBytes.getDescriptor().getOneofs()).hasSize(1);
    assertThat(NullableBytes.getDescriptor().getOneofs().getFirst().getFields())
        .extracting(FieldDescriptor::getName)
        .containsExactly("data", "null_value");
    assertThat(PutRequest.getDescriptor().findFieldByName("value"))
        .extracting(field -> field.getMessageType().getFullName())
        .isEqualTo("kv.v1.NullableBytes");
  }
}
