package io.kvservice.application.storage;

import io.kvservice.application.FailureCode;
import io.kvservice.application.KvServiceException;

public final class StorageAccessException extends KvServiceException {

  private StorageAccessException(FailureCode failureCode, String message, Throwable cause) {
    super(failureCode, message, cause);
  }

  public static StorageAccessException unavailable(String message, Throwable cause) {
    return new StorageAccessException(FailureCode.UNAVAILABLE, message, cause);
  }

  public static StorageAccessException deadlineExceeded(String message, Throwable cause) {
    return new StorageAccessException(FailureCode.DEADLINE_EXCEEDED, message, cause);
  }

  public static StorageAccessException cancelled(String message, Throwable cause) {
    return new StorageAccessException(FailureCode.CANCELLED, message, cause);
  }

  public static StorageAccessException internal(String message) {
    return new StorageAccessException(FailureCode.INTERNAL, message, null);
  }

  public static StorageAccessException internal(String message, Throwable cause) {
    return new StorageAccessException(FailureCode.INTERNAL, message, cause);
  }
}
