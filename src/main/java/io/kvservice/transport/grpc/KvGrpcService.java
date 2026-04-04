package io.kvservice.transport.grpc;

import java.util.function.Supplier;

import io.grpc.stub.StreamObserver;
import io.kvservice.api.v1.DeleteRequest;
import io.kvservice.api.v1.DeleteResponse;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.GetResponse;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.PutRequest;
import io.kvservice.api.v1.PutResponse;
import io.kvservice.application.DeleteValueUseCase;
import io.kvservice.application.GetValueUseCase;
import io.kvservice.application.PutValueUseCase;

public final class KvGrpcService extends KvServiceGrpc.KvServiceImplBase {

    private final PutValueUseCase putValueUseCase;
    private final GetValueUseCase getValueUseCase;
    private final DeleteValueUseCase deleteValueUseCase;
    private final GrpcNullableBytesMapper valueMapper;
    private final GrpcUnaryRequestBudgetFactory requestBudgetFactory;
    private final GrpcStatusTranslator statusTranslator;

    public KvGrpcService(
            PutValueUseCase putValueUseCase,
            GetValueUseCase getValueUseCase,
            DeleteValueUseCase deleteValueUseCase,
            GrpcNullableBytesMapper valueMapper,
            GrpcUnaryRequestBudgetFactory requestBudgetFactory,
            GrpcStatusTranslator statusTranslator
    ) {
        this.putValueUseCase = putValueUseCase;
        this.getValueUseCase = getValueUseCase;
        this.deleteValueUseCase = deleteValueUseCase;
        this.valueMapper = valueMapper;
        this.requestBudgetFactory = requestBudgetFactory;
        this.statusTranslator = statusTranslator;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        executeUnary(responseObserver, () -> {
            this.putValueUseCase.execute(
                    request.getKey(),
                    this.valueMapper.fromRequestValue(request.getValue(), request.hasValue()),
                    this.requestBudgetFactory.create()
            );
            return PutResponse.getDefaultInstance();
        });
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        executeUnary(responseObserver, () -> GetResponse.newBuilder()
                .setRecord(this.valueMapper.toRecord(this.getValueUseCase.execute(request.getKey(), this.requestBudgetFactory.create())))
                .build());
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        executeUnary(responseObserver, () -> {
            this.deleteValueUseCase.execute(request.getKey(), this.requestBudgetFactory.create());
            return DeleteResponse.getDefaultInstance();
        });
    }

    private <T> void executeUnary(StreamObserver<T> responseObserver, Supplier<T> action) {
        try {
            T response = action.get();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        catch (Throwable failure) {
            responseObserver.onError(this.statusTranslator.translate(failure));
        }
    }
}
