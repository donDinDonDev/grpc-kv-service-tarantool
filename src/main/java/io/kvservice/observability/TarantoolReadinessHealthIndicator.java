package io.kvservice.observability;

import com.fasterxml.jackson.core.type.TypeReference;
import io.tarantool.client.box.TarantoolBoxClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public final class TarantoolReadinessHealthIndicator implements HealthIndicator {

  private static final String READINESS_SCRIPT = "return box.space.KV ~= nil";

  private final TarantoolBoxClient client;
  private final Duration requestTimeout;

  public TarantoolReadinessHealthIndicator(TarantoolBoxClient client, Duration requestTimeout) {
    this.client = client;
    this.requestTimeout = requestTimeout;
  }

  @Override
  public Health health() {
    try {
      List<Boolean> response =
          this.client
              .eval(READINESS_SCRIPT, new TypeReference<List<Boolean>>() {})
              .get(Math.max(1L, this.requestTimeout.toMillis()), TimeUnit.MILLISECONDS)
              .get();
      if (!response.isEmpty() && Boolean.TRUE.equals(response.getFirst())) {
        return Health.up().withDetail("storage", "tarantool").withDetail("space", "KV").build();
      }
      return Health.outOfService()
          .withDetail("storage", "tarantool")
          .withDetail("space", "KV")
          .withDetail("reason", "KV space is unavailable")
          .build();
    } catch (Exception failure) {
      return Health.down(failure)
          .withDetail("storage", "tarantool")
          .withDetail("space", "KV")
          .build();
    }
  }
}
