package io.kvservice.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.application.storage.RangeBatchQuery;
import io.kvservice.application.storage.StoredEntry;
import io.kvservice.application.storage.StoredValue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class RangeValueUseCaseTest {

  private final KeyValueValidator validator =
      new KeyValueValidator(DataSize.ofBytes(32), DataSize.ofBytes(1024));

  @Test
  void rejectsEqualOrDescendingBoundsAsInvalidArgument() {
    ScriptedStorage storage = new ScriptedStorage(List.of());
    RangeValueUseCase useCase = new RangeValueUseCase(storage, this.validator, 2);

    assertThatThrownBy(
            () ->
                useCase.stream(
                    "same",
                    "same",
                    RequestBudget.of(Duration.ofSeconds(1), () -> false),
                    item -> {}))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("key_since must be less than key_to");

    assertThat(storage.rangeQueries).isEmpty();
  }

  @Test
  void rejectsBoundsThatAreDescendingInUtf8LexicographicOrder() {
    ScriptedStorage storage = new ScriptedStorage(List.of());
    RangeValueUseCase useCase = new RangeValueUseCase(storage, this.validator, 2);

    assertThatThrownBy(
            () ->
                useCase.stream(
                    key(0x10000),
                    key(0xE000),
                    RequestBudget.of(Duration.ofSeconds(1), () -> false),
                    item -> {}))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("key_since must be less than key_to");

    assertThat(storage.rangeQueries).isEmpty();
  }

  @Test
  void returnsEmptyStreamForValidRangeWithoutResults() {
    ScriptedStorage storage = new ScriptedStorage(List.of(List.<StoredEntry>of()));
    RangeValueUseCase useCase = new RangeValueUseCase(storage, this.validator, 2);
    List<StoredEntry> emitted = new ArrayList<>();

    useCase.stream("a", "c", RequestBudget.of(Duration.ofSeconds(1), () -> false), emitted::add);

    assertThat(emitted).isEmpty();
    RangeBatchQuery query = storage.rangeQueries.getFirst();
    assertThat(query.keyFromInclusive()).isEqualTo("a");
    assertThat(query.keyToExclusive()).isEqualTo("c");
    assertThat(query.startAfter()).isNull();
    assertThat(query.limit()).isEqualTo(2);
    assertThat(storage.rangeTimeouts.getFirst()).isPositive();
  }

  @Test
  void streamsNullValuesInBatchesAndResumesFromLastDeliveredKey() {
    ScriptedStorage storage =
        new ScriptedStorage(
            List.of(
                List.of(new StoredEntry("b", StoredValue.nullValue())),
                List.of(new StoredEntry("c", StoredValue.bytes(new byte[0]))),
                List.<StoredEntry>of()));
    RangeValueUseCase useCase = new RangeValueUseCase(storage, this.validator, 1);
    List<StoredEntry> emitted = new ArrayList<>();

    useCase.stream("b", "d", RequestBudget.of(Duration.ofSeconds(1), () -> false), emitted::add);

    assertThat(emitted).extracting(StoredEntry::key).containsExactly("b", "c");
    assertThat(emitted.getFirst().value().isNull()).isTrue();
    assertThat(((StoredValue.BytesValue) emitted.get(1).value()).bytes()).isEmpty();
    assertThat(storage.rangeQueries)
        .extracting(RangeBatchQuery::startAfter)
        .containsExactly(null, "b", "c");
  }

  @Test
  void stopsBeforeReadingNextBatchWhenRequestIsCancelled() {
    AtomicBoolean cancelled = new AtomicBoolean();
    ScriptedStorage storage =
        new ScriptedStorage(
            List.of(
                List.of(new StoredEntry("b", StoredValue.bytes(new byte[] {1}))),
                List.of(new StoredEntry("c", StoredValue.bytes(new byte[] {2})))));
    RangeValueUseCase useCase = new RangeValueUseCase(storage, this.validator, 1);

    assertThatThrownBy(
            () ->
                useCase.stream(
                    "a",
                    "d",
                    RequestBudget.of(Duration.ofSeconds(1), cancelled::get),
                    entry -> cancelled.set(true)))
        .isInstanceOf(RequestCancelledException.class)
        .hasMessage("request cancelled");

    assertThat(storage.rangeQueries).hasSize(1);
  }

  @Test
  void rejectsNonPositiveBatchSize() {
    assertThatThrownBy(
            () -> new RangeValueUseCase(new ScriptedStorage(List.of()), this.validator, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("batchSize must be positive");
  }

  private static String key(int codePoint) {
    return new String(Character.toChars(codePoint));
  }

  private static final class ScriptedStorage implements KeyValueStoragePort {

    private final List<List<StoredEntry>> scriptedBatches;
    private final List<RangeBatchQuery> rangeQueries = new ArrayList<>();
    private final List<Duration> rangeTimeouts = new ArrayList<>();
    private int batchIndex;

    private ScriptedStorage(List<List<StoredEntry>> scriptedBatches) {
      this.scriptedBatches = scriptedBatches;
    }

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
      throw new UnsupportedOperationException();
    }

    @Override
    public List<StoredEntry> getRangeBatch(RangeBatchQuery query, Duration timeout) {
      this.rangeQueries.add(query);
      this.rangeTimeouts.add(timeout);
      if (this.batchIndex >= this.scriptedBatches.size()) {
        return List.of();
      }
      return this.scriptedBatches.get(this.batchIndex++);
    }
  }
}
