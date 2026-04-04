package io.kvservice.transport.grpc;

import java.util.function.Supplier;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.kvservice.api.v1.CountRequest;
import io.kvservice.api.v1.CountResponse;
import io.grpc.stub.StreamObserver;
import io.kvservice.api.v1.DeleteRequest;
import io.kvservice.api.v1.DeleteResponse;
import io.kvservice.api.v1.GetRequest;
import io.kvservice.api.v1.GetResponse;
import io.kvservice.api.v1.KvServiceGrpc;
import io.kvservice.api.v1.PutRequest;
import io.kvservice.api.v1.PutResponse;
import io.kvservice.api.v1.RangeItem;
import io.kvservice.api.v1.RangeRequest;
import io.kvservice.application.CountEntriesUseCase;
import io.kvservice.application.DeleteValueUseCase;
import io.kvservice.application.GetValueUseCase;
import io.kvservice.application.PutValueUseCase;
import io.kvservice.application.RangeValueUseCase;
import io.kvservice.application.RequestBudget;
import io.kvservice.observability.RangeStreamMetrics;

public final class KvGrpcService extends KvServiceGrpc.KvServiceImplBase {

    private final PutValueUseCase putValueUseCase;
    private final GetValueUseCase getValueUseCase;
    private final DeleteValueUseCase deleteValueUseCase;
    private final RangeValueUseCase rangeValueUseCase;
    private final CountEntriesUseCase countEntriesUseCase;
    private final GrpcNullableBytesMapper valueMapper;
    private final GrpcUnaryRequestBudgetFactory requestBudgetFactory;
    private final GrpcCountRequestBudgetFactory countRequestBudgetFactory;
    private final GrpcStatusTranslator statusTranslator;
    private final RangeStreamPermitLimiter rangeStreamPermitLimiter;
    private final RangeStreamMetrics rangeStreamMetrics;

    public KvGrpcService(
            PutValueUseCase putValueUseCase,
            GetValueUseCase getValueUseCase,
            DeleteValueUseCase deleteValueUseCase,
            RangeValueUseCase rangeValueUseCase,
            CountEntriesUseCase countEntriesUseCase,
            GrpcNullableBytesMapper valueMapper,
            GrpcUnaryRequestBudgetFactory requestBudgetFactory,
            GrpcCountRequestBudgetFactory countRequestBudgetFactory,
            GrpcStatusTranslator statusTranslator,
            RangeStreamPermitLimiter rangeStreamPermitLimiter,
            RangeStreamMetrics rangeStreamMetrics
    ) {
        this.putValueUseCase = putValueUseCase;
        this.getValueUseCase = getValueUseCase;
        this.deleteValueUseCase = deleteValueUseCase;
        this.rangeValueUseCase = rangeValueUseCase;
        this.countEntriesUseCase = countEntriesUseCase;
        this.valueMapper = valueMapper;
        this.requestBudgetFactory = requestBudgetFactory;
        this.countRequestBudgetFactory = countRequestBudgetFactory;
        this.statusTranslator = statusTranslator;
        this.rangeStreamPermitLimiter = rangeStreamPermitLimiter;
        this.rangeStreamMetrics = rangeStreamMetrics;
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

    @Override
    public void range(RangeRequest request, StreamObserver<RangeItem> responseObserver) {
        RangeStreamPermitLimiter.Permit permit = null;
        RangeStreamMetrics.ActiveStream activeStream = null;
        Status.Code statusCode = Status.Code.OK;
        try {
            permit = this.rangeStreamPermitLimiter.acquire();
            activeStream = this.rangeStreamMetrics.startStream();
            RangeStreamMetrics.ActiveStream streamMetrics = activeStream;
            RequestBudget requestBudget = this.requestBudgetFactory.create();
            GrpcRangeResponseWriter responseWriter = new GrpcRangeResponseWriter(serverObserver(responseObserver), requestBudget);
            this.rangeValueUseCase.stream(
                    request.getKeySince(),
                    request.getKeyTo(),
                    requestBudget,
                    entry -> {
                        responseWriter.write(RangeItem.newBuilder()
                                .setRecord(this.valueMapper.toRecord(entry))
                                .build());
                        streamMetrics.recordItem();
                    }
            );
            responseObserver.onCompleted();
        }
        catch (Throwable failure) {
            StatusRuntimeException translatedFailure = this.statusTranslator.translate(failure);
            statusCode = translatedFailure.getStatus().getCode();
            responseObserver.onError(translatedFailure);
        }
        finally {
            if (activeStream != null) {
                activeStream.complete(statusCode);
            }
            if (permit != null) {
                permit.close();
            }
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        executeUnary(responseObserver, () -> CountResponse.newBuilder()
                .setCount(this.countEntriesUseCase.execute(this.countRequestBudgetFactory.create()))
                .build());
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

    @SuppressWarnings("unchecked")
    private ServerCallStreamObserver<RangeItem> serverObserver(StreamObserver<RangeItem> responseObserver) {
        return (ServerCallStreamObserver<RangeItem>) responseObserver;
    }
}
