package io.kvservice.observability;

import io.micrometer.tracing.Span;
import java.util.Map;

final class TraceLogFields {

  private TraceLogFields() {}

  static void put(Map<String, String> fields, Span span) {
    if (fields == null || span == null || span.context() == null) {
      return;
    }
    putIfPresent(fields, "traceId", span.context().traceId());
    putIfPresent(fields, "spanId", span.context().spanId());
  }

  private static void putIfPresent(Map<String, String> fields, String key, String value) {
    if (value != null && !value.isBlank()) {
      fields.put(key, value);
    }
  }
}
