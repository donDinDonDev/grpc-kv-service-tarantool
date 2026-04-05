package io.kvservice.application;

public final class RequestDeadlineExceededException extends KvServiceException {

  public RequestDeadlineExceededException(String message) {
    super(FailureCode.DEADLINE_EXCEEDED, message);
  }
}
