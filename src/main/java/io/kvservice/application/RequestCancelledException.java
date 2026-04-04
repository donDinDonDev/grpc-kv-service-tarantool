package io.kvservice.application;

public final class RequestCancelledException extends KvServiceException {

    public RequestCancelledException(String message) {
        super(FailureCode.CANCELLED, message);
    }
}
