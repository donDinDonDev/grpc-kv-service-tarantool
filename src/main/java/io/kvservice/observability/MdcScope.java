package io.kvservice.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.MDC;

final class MdcScope implements AutoCloseable {

  private static final MdcScope NOOP = new MdcScope(List.of());

  private final List<MDC.MDCCloseable> closeables;

  private MdcScope(List<MDC.MDCCloseable> closeables) {
    this.closeables = closeables;
  }

  static MdcScope open(Map<String, String> fields) {
    if (fields == null || fields.isEmpty()) {
      return NOOP;
    }
    List<MDC.MDCCloseable> closeables = new ArrayList<>(fields.size());
    fields.forEach(
        (key, value) -> {
          if (value != null) {
            closeables.add(MDC.putCloseable(key, value));
          }
        });
    if (closeables.isEmpty()) {
      return NOOP;
    }
    return new MdcScope(closeables);
  }

  @Override
  public void close() {
    for (int index = this.closeables.size() - 1; index >= 0; index--) {
      this.closeables.get(index).close();
    }
  }
}
