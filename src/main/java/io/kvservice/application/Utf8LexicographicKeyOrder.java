package io.kvservice.application;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class Utf8LexicographicKeyOrder {

  private Utf8LexicographicKeyOrder() {}

  public static int compare(String left, String right) {
    return Arrays.compareUnsigned(
        left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
  }
}
