package io.kvservice.application;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.kvservice.application.storage.StoredValue;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class KeyValueValidatorTest {

  private final KeyValueValidator validator =
      new KeyValueValidator(DataSize.ofBytes(4), DataSize.ofBytes(3));

  @Test
  void rejectsMissingEmptyControlAndOversizedKeys() {
    assertThatThrownBy(() -> this.validator.validateKey(null))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("key is required");

    assertThatThrownBy(() -> this.validator.validateKey(""))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("key must not be empty");

    assertThatThrownBy(() -> this.validator.validateKey("ab\n"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("key must not contain control characters");

    assertThatThrownBy(() -> this.validator.validateKey("абв"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("key exceeds max size");
  }

  @Test
  void requiresExplicitValueAndRejectsOversizedPayload() {
    assertThatThrownBy(() -> this.validator.requireExplicitValue(Optional.empty()))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("value is required");

    assertThatThrownBy(
            () -> this.validator.validateValue(StoredValue.bytes(new byte[] {1, 2, 3, 4})))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("value exceeds max size");
  }
}
