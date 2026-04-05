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
    assertThatCode(() -> new RangeBatchQuery("a", "c", "a", 2)).doesNotThrowAnyException();
  }

  @Test
  void usesUtf8LexicographicOrderingForRangeBounds() {
    String utf8LowerKey = key(0xE000);
    String utf8UpperKey = key(0x10000);

    assertThatCode(() -> new RangeBatchQuery(utf8LowerKey, utf8UpperKey, utf8LowerKey, 2))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> new RangeBatchQuery(utf8UpperKey, utf8LowerKey, null, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("keyFromInclusive must be less than keyToExclusive");
  }

  private static String key(int codePoint) {
    return new String(Character.toChars(codePoint));
  }
}
