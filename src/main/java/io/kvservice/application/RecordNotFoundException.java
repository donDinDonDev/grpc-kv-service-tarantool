package io.kvservice.application;

public final class RecordNotFoundException extends KvServiceException {

    public RecordNotFoundException(String message) {
        super(FailureCode.NOT_FOUND, message);
    }
}
