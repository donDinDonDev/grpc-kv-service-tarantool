package io.kvservice.observability;

import com.google.protobuf.MessageLite;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public final class GrpcResponseSizeLimitInterceptor implements ServerInterceptor {

  private final long maxResponseBytes;

  public GrpcResponseSizeLimitInterceptor(long maxResponseBytes) {
    if (maxResponseBytes <= 0) {
      throw new IllegalArgumentException("maxResponseBytes must be positive");
    }
    this.maxResponseBytes = maxResponseBytes;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    ServerCall<ReqT, RespT> forwardingCall =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void sendMessage(RespT message) {
            if (message instanceof MessageLite messageLite
                && messageLite.getSerializedSize() > maxResponseBytes) {
              throw Status.RESOURCE_EXHAUSTED
                  .withDescription("gRPC response message exceeds configured max-response-bytes")
                  .asRuntimeException();
            }
            super.sendMessage(message);
          }
        };
    return next.startCall(forwardingCall, headers);
  }
}
