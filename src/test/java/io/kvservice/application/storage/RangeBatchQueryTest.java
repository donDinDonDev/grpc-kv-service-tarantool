package io.kvservice.application.storage;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RangeBatchQueryTest {

    @Test
    void rejectsEqualOrDescendingRangeBounds() {
        assertThatThrownBy(() -> new RangeBatchQuery("same", "same", null, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyFromInclusive must be less than keyToExclusive");

        assertThatThrownBy(() -> new RangeBatchQuery("z", "a", null, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyFromInclusive must be less than keyToExclusive");
    }

    @Test
    void allowsValidHalfOpenRangeWithResumeCursor() {
        assertThatCode(() -> new RangeBatchQuery("a", "c", "a", 2))
                .doesNotThrowAnyException();
    }
}
