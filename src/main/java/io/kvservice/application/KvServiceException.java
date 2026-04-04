package io.kvservice.application;

import java.util.Objects;

public abstract class KvServiceException extends RuntimeException {

    private final FailureCode failureCode;

    protected KvServiceException(FailureCode failureCode, String message) {
        super(message);
        this.failureCode = Objects.requireNonNull(failureCode, "failureCode");
    }

    protected KvServiceException(FailureCode failureCode, String message, Throwable cause) {
        super(message, cause);
        this.failureCode = Objects.requireNonNull(failureCode, "failureCode");
    }

    public FailureCode failureCode() {
        return this.failureCode;
    }
}
