package io.kvservice.observability;

import java.util.Map;

final class StructuredLogMessage {

  private StructuredLogMessage() {}

  static String of(String event, Map<String, String> fields) {
    StringBuilder result = new StringBuilder(128);
    result.append('{');
    appendField(result, "event", event, true);
    if (fields != null) {
      for (Map.Entry<String, String> entry : fields.entrySet()) {
        if (entry.getValue() != null) {
          appendField(result, entry.getKey(), entry.getValue(), false);
        }
      }
    }
    result.append('}');
    return result.toString();
  }

  private static void appendField(StringBuilder result, String key, String value, boolean first) {
    if (!first) {
      result.append(',');
    }
    appendQuoted(result, key);
    result.append(':');
    appendQuoted(result, value);
  }

  private static void appendQuoted(StringBuilder result, String value) {
    result.append('"');
    for (int index = 0; index < value.length(); index++) {
      char current = value.charAt(index);
      switch (current) {
        case '\\' -> result.append("\\\\");
        case '"' -> result.append("\\\"");
        case '\b' -> result.append("\\b");
        case '\f' -> result.append("\\f");
        case '\n' -> result.append("\\n");
        case '\r' -> result.append("\\r");
        case '\t' -> result.append("\\t");
        default -> {
          if (current < 0x20) {
            result.append("\\u");
            String hex = Integer.toHexString(current);
            for (int padding = hex.length(); padding < 4; padding++) {
              result.append('0');
            }
            result.append(hex);
          } else {
            result.append(current);
          }
        }
      }
    }
    result.append('"');
  }
}
