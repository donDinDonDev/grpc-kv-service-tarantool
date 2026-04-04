package io.kvservice.application;

public final class ServiceUnavailableException extends KvServiceException {

    public ServiceUnavailableException(String message) {
        super(FailureCode.UNAVAILABLE, message);
    }
}
