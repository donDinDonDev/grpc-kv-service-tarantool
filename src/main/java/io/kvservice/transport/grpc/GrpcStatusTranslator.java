package io.kvservice.transport.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kvservice.application.FailureCode;
import io.kvservice.application.KvServiceException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public final class GrpcStatusTranslator {

  public StatusRuntimeException translate(Throwable failure) {
    Throwable rootCause = unwrap(failure);
    if (rootCause instanceof StatusRuntimeException statusRuntimeException) {
      return statusRuntimeException;
    }
    if (rootCause instanceof KvServiceException serviceException) {
      return statusFor(serviceException.failureCode(), descriptionFor(serviceException))
          .asRuntimeException();
    }
    return Status.INTERNAL.withDescription("internal error").asRuntimeException();
  }

  private String descriptionFor(KvServiceException serviceException) {
    if (serviceException.failureCode() == FailureCode.INTERNAL) {
      return "internal error";
    }
    return serviceException.getMessage();
  }

  private Status statusFor(FailureCode failureCode, String description) {
    return switch (failureCode) {
      case INVALID_ARGUMENT -> Status.INVALID_ARGUMENT.withDescription(description);
      case NOT_FOUND -> Status.NOT_FOUND.withDescription(description);
      case UNAVAILABLE -> Status.UNAVAILABLE.withDescription(description);
      case DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED.withDescription(description);
      case CANCELLED -> Status.CANCELLED.withDescription(description);
      case INTERNAL -> Status.INTERNAL.withDescription(description);
    };
  }

  private Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }
}
