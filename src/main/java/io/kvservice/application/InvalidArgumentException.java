package io.kvservice.application;

public final class InvalidArgumentException extends KvServiceException {

    public InvalidArgumentException(String message) {
        super(FailureCode.INVALID_ARGUMENT, message);
    }
}
