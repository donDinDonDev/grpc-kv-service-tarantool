package io.kvservice.storage.tarantool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CancellationException;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kvservice.application.RequestDeadlineExceededException;
import io.kvservice.application.Utf8LexicographicKeyOrder;
import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.RangeBatchQuery;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import io.kvservice.application.storage.StorageAccessException;
import io.kvservice.observability.SafeLogFields;
import io.kvservice.observability.TarantoolObservability;
import io.tarantool.client.BaseOptions;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.DeleteOptions;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.Tuple;

public final class TarantoolKeyValueStorage implements KeyValueStoragePort {

    private static final String SPACE_NAME = "KV";
    private static final String COUNT_SCRIPT = "return box.space.KV:count()";

    private final TarantoolBoxClient client;
    private final TarantoolTupleMapper tupleMapper;
    private final Duration defaultRequestTimeout;
    private final TarantoolObservability observability;

    public TarantoolKeyValueStorage(
            TarantoolBoxClient client,
            Duration requestTimeout,
            TarantoolObservability observability
    ) {
        this.client = client;
        this.defaultRequestTimeout = requestTimeout;
        this.tupleMapper = new TarantoolTupleMapper();
        this.observability = observability;
    }

    @Override
    public void put(String key, StoredValue value, Duration timeout) {
        this.observability.observe("put", SafeLogFields.forPut(key, value), () -> {
            await(space().replace(tupleFor(key, value), BaseOptions.builder()
                    .withTimeout(timeoutMillis(timeout))
                    .build()), timeout);
            return null;
        });
    }

    @Override
    public Optional<StoredEntry> get(String key, Duration timeout) {
        return this.observability.observe("get", SafeLogFields.forKey("key", key), () -> {
            SelectResponse<List<Tuple<List<?>>>> response = await(space().select(List.of(key), SelectOptions.builder()
                    .withTimeout(timeoutMillis(timeout))
                    .build()), timeout);
            List<Tuple<List<?>>> tuples = response.get();
            if (tuples.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(this.tupleMapper.toStoredEntry(tuples.getFirst()));
        });
    }

    @Override
    public void delete(String key, Duration timeout) {
        this.observability.observe("delete", SafeLogFields.forKey("key", key), () -> {
            await(space().delete(List.of(key), DeleteOptions.builder()
                    .withTimeout(timeoutMillis(timeout))
                    .build()), timeout);
            return null;
        });
    }

    @Override
    public long count(Duration timeout) {
        return this.observability.observe("count", Map.of(), () -> {
            List<Long> values = await(this.client.eval(COUNT_SCRIPT, new TypeReference<List<Long>>() {
            }), timeout).get();
            if (values.isEmpty() || values.getFirst() == null) {
                throw StorageAccessException.internal("Tarantool returned null for count()");
            }
            return values.getFirst();
        });
    }

    @Override
    public List<StoredEntry> getRangeBatch(RangeBatchQuery query, Duration timeout) {
        return this.observability.observe("range_batch",
                SafeLogFields.forRange(query.keyFromInclusive(), query.keyToExclusive()),
                () -> {
                    RangeScanStart scanStart = resolveScanStart(query);
                    SelectOptions options = SelectOptions.builder()
                            .withLimit(query.limit())
                            .withTimeout(timeoutMillis(timeout))
                            .withIterator(scanStart.iterator())
                            .build();

                    SelectResponse<List<Tuple<List<?>>>> response =
                            await(space().select(List.of(scanStart.key()), options), timeout);
                    List<StoredEntry> result = new ArrayList<>(response.get().size());
                    for (Tuple<List<?>> tuple : response.get()) {
                        StoredEntry entry = this.tupleMapper.toStoredEntry(tuple);
                        if (Utf8LexicographicKeyOrder.compare(entry.key(), query.keyToExclusive()) >= 0) {
                            break;
                        }
                        result.add(entry);
                    }
                    return List.copyOf(result);
                });
    }

    private TarantoolBoxSpace space() {
        return this.client.space(SPACE_NAME);
    }

    private List<Object> tupleFor(String key, StoredValue value) {
        Object rawValue = value instanceof StoredValue.BytesValue bytesValue ? bytesValue.bytes() : null;
        return Arrays.asList(key, rawValue);
    }

    private RangeScanStart resolveScanStart(RangeBatchQuery query) {
        String startAfter = query.startAfter();
        if (startAfter == null || Utf8LexicographicKeyOrder.compare(startAfter, query.keyFromInclusive()) < 0) {
            return new RangeScanStart(query.keyFromInclusive(), BoxIterator.GE);
        }
        return new RangeScanStart(startAfter, BoxIterator.GT);
    }

    private <T> T await(java.util.concurrent.CompletableFuture<T> future) {
        try {
            return future.get(this.defaultRequestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw StorageAccessException.cancelled("Tarantool request was interrupted", exception);
        }
        catch (ExecutionException | TimeoutException exception) {
            throw StorageAccessException.unavailable("Tarantool request failed", exception);
        }
    }

    private <T> T await(java.util.concurrent.CompletableFuture<T> future, Duration timeout) {
        try {
            return future.get(timeoutMillis(timeout), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw StorageAccessException.cancelled("Tarantool request was interrupted", exception);
        }
        catch (CancellationException exception) {
            future.cancel(true);
            throw StorageAccessException.cancelled("Tarantool request was cancelled", exception);
        }
        catch (TimeoutException exception) {
            future.cancel(true);
            throw StorageAccessException.deadlineExceeded("Tarantool request timed out", exception);
        }
        catch (ExecutionException exception) {
            future.cancel(true);
            throw StorageAccessException.unavailable("Tarantool request failed", exception.getCause());
        }
    }

    private long timeoutMillis(Duration timeout) {
        Duration effectiveTimeout = timeout == null ? this.defaultRequestTimeout : timeout;
        if (effectiveTimeout.isNegative() || effectiveTimeout.isZero()) {
            throw new RequestDeadlineExceededException("deadline exceeded");
        }
        return Math.max(1L, effectiveTimeout.toMillis());
    }

    private record RangeScanStart(String key, BoxIterator iterator) {
    }
}
