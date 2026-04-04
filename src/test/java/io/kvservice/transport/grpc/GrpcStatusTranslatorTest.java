package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.kvservice.application.InvalidArgumentException;
import io.kvservice.application.RecordNotFoundException;
import io.kvservice.application.RequestCancelledException;
import io.kvservice.application.RequestDeadlineExceededException;
import io.kvservice.application.storage.StorageAccessException;
import org.junit.jupiter.api.Test;

class GrpcStatusTranslatorTest {

    private final GrpcStatusTranslator translator = new GrpcStatusTranslator();

    @Test
    void mapsTypedInternalErrorsToExpectedGrpcStatuses() {
        assertThat(this.translator.translate(new InvalidArgumentException("bad request")).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(this.translator.translate(new RecordNotFoundException("missing")).getStatus().getCode())
                .isEqualTo(Status.Code.NOT_FOUND);
        assertThat(this.translator.translate(StorageAccessException.unavailable("db down", new RuntimeException("x"))).getStatus().getCode())
                .isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(this.translator.translate(new RequestDeadlineExceededException("too late")).getStatus().getCode())
                .isEqualTo(Status.Code.DEADLINE_EXCEEDED);
        assertThat(this.translator.translate(new RequestCancelledException("cancelled")).getStatus().getCode())
                .isEqualTo(Status.Code.CANCELLED);
        assertThat(this.translator.translate(StorageAccessException.internal("broken state")).getStatus().getCode())
                .isEqualTo(Status.Code.INTERNAL);
    }

    @Test
    void hidesUnexpectedFailureDetailsBehindInternalStatus() {
        assertThat(this.translator.translate(new IllegalStateException("boom")).getStatus())
                .extracting(Status::getCode, Status::getDescription)
                .containsExactly(Status.Code.INTERNAL, "internal error");
    }

    @Test
    void hidesTypedInternalFailureDetailsBehindInternalStatus() {
        assertThat(this.translator.translate(StorageAccessException.internal("broken state")).getStatus())
                .extracting(Status::getCode, Status::getDescription)
                .containsExactly(Status.Code.INTERNAL, "internal error");
    }
}
