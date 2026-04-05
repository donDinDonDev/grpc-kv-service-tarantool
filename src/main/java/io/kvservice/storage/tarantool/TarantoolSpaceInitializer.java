package io.kvservice.storage.tarantool;

import io.kvservice.application.storage.StorageAccessException;
import io.tarantool.client.box.TarantoolBoxClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.core.io.Resource;

public final class TarantoolSpaceInitializer {

  private final TarantoolBoxClient client;
  private final Resource scriptLocation;
  private final String engine;
  private final Duration requestTimeout;

  public TarantoolSpaceInitializer(
      TarantoolBoxClient client, Resource scriptLocation, String engine, Duration requestTimeout) {
    this.client = client;
    this.scriptLocation = scriptLocation;
    this.engine = engine;
    this.requestTimeout = requestTimeout;
  }

  public void initialize() {
    try {
      String script = this.scriptLocation.getContentAsString(StandardCharsets.UTF_8);
      this.client
          .eval(script, List.of(this.engine))
          .get(this.requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (IOException exception) {
      throw StorageAccessException.internal("failed to load Tarantool init script", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw StorageAccessException.cancelled("Tarantool init was interrupted", exception);
    } catch (ExecutionException | TimeoutException exception) {
      throw StorageAccessException.unavailable("Tarantool init failed", exception);
    }
  }
}
