package io.kvservice.transport.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kvservice.api.v1.RangeItem;
import io.kvservice.api.v1.RangeRequest;
import io.kvservice.application.CountEntriesUseCase;
import io.kvservice.application.DeleteValueUseCase;
import io.kvservice.application.GetValueUseCase;
import io.kvservice.application.KeyValueValidator;
import io.kvservice.application.PutValueUseCase;
import io.kvservice.application.RangeValueUseCase;
import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.RangeBatchQuery;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class KvGrpcServiceRangeTest {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void shutdownExecutor() {
        this.executor.shutdownNow();
    }

    @Test
    void enforcesConfiguredActiveRangeStreamLimitAndReleasesPermitOnCancellation() throws Exception {
        BlockingRangeStorage storage = new BlockingRangeStorage();
        KeyValueValidator validator = new KeyValueValidator(DataSize.ofBytes(32), DataSize.ofBytes(1024));
        KvGrpcService service = new KvGrpcService(
                new PutValueUseCase(storage, validator),
                new GetValueUseCase(storage, validator),
                new DeleteValueUseCase(storage, validator),
                new RangeValueUseCase(storage, validator, 1),
                new CountEntriesUseCase(storage),
                new GrpcNullableBytesMapper(),
                new GrpcUnaryRequestBudgetFactory(Duration.ofSeconds(3)),
                new GrpcCountRequestBudgetFactory(Duration.ofSeconds(15)),
                new GrpcStatusTranslator(),
                new RangeStreamPermitLimiter(1)
        );

        TestServerCallStreamObserver<RangeItem> firstObserver = new TestServerCallStreamObserver<>();
        firstObserver.setReady(false);
        Context.CancellableContext firstContext = Context.current().withCancellation();
        Future<?> firstCall = this.executor.submit(() -> firstContext.run(() -> service.range(
                RangeRequest.newBuilder().setKeySince("a").setKeyTo("z").build(),
                firstObserver
        )));

        assertThat(storage.firstBatchStarted.await(5, TimeUnit.SECONDS)).isTrue();

        TestServerCallStreamObserver<RangeItem> secondObserver = new TestServerCallStreamObserver<>();
        service.range(RangeRequest.newBuilder().setKeySince("a").setKeyTo("z").build(), secondObserver);

        assertThat(secondObserver.error()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) secondObserver.error()).getStatus().getCode())
                .isEqualTo(Status.Code.UNAVAILABLE);

        firstContext.cancel(null);
        firstCall.get(5, TimeUnit.SECONDS);

        assertThat(firstObserver.error()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) firstObserver.error()).getStatus().getCode())
                .isEqualTo(Status.Code.CANCELLED);

        TestServerCallStreamObserver<RangeItem> thirdObserver = new TestServerCallStreamObserver<>();
        service.range(RangeRequest.newBuilder().setKeySince("a").setKeyTo("z").build(), thirdObserver);

        assertThat(thirdObserver.completed()).isTrue();
        assertThat(thirdObserver.error()).isNull();
        assertThat(storage.rangeRequests).hasSize(2);
    }

    private static final class BlockingRangeStorage implements KeyValueStoragePort {

        private final CountDownLatch firstBatchStarted = new CountDownLatch(1);
        private final List<RangeBatchQuery> rangeRequests = new java.util.concurrent.CopyOnWriteArrayList<>();
        private int callCount;

        @Override
        public void put(String key, StoredValue value, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<StoredEntry> get(String key, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(String key, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(Duration timeout) {
            return 0;
        }

        @Override
        public List<StoredEntry> getRangeBatch(RangeBatchQuery query, Duration timeout) {
            this.rangeRequests.add(query);
            this.callCount++;
            if (this.callCount == 1) {
                this.firstBatchStarted.countDown();
                return List.of(new StoredEntry("b", StoredValue.nullValue()));
            }
            return List.of();
        }
    }
}
