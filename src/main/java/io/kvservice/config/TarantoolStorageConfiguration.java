package io.kvservice.config;

import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.observability.TarantoolObservability;
import io.kvservice.observability.TarantoolReadinessHealthIndicator;
import io.kvservice.storage.tarantool.TarantoolKeyValueStorage;
import io.kvservice.storage.tarantool.TarantoolSpaceInitializer;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "kvservice.tarantool", name = "enabled", havingValue = "true", matchIfMissing = true)
public class TarantoolStorageConfiguration {

    @Bean(destroyMethod = "close")
    TarantoolBoxClient tarantoolBoxClient(KvServiceProperties properties, TarantoolObservability observability)
            throws Exception {
        KvServiceProperties.Tarantool tarantool = properties.getTarantool();
        return TarantoolFactory.box()
                .withHost(tarantool.getHost())
                .withPort(tarantool.getPort())
                .withUser(tarantool.getUsername())
                .withPassword(tarantool.getPassword())
                .withConnectTimeout(tarantool.getConnectTimeout().toMillis())
                .withReconnectAfter(tarantool.getReconnectAfter().toMillis())
                .withPoolEventListener(observability)
                .withFetchSchema(true)
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "kvservice.tarantool.init", name = "enabled", havingValue = "true", matchIfMissing = true)
    TarantoolSpaceInitializer tarantoolSpaceInitializer(
            TarantoolBoxClient client,
            KvServiceProperties properties
    ) {
        KvServiceProperties.Tarantool tarantool = properties.getTarantool();
        TarantoolSpaceInitializer initializer = new TarantoolSpaceInitializer(
                client,
                tarantool.getInit().getScriptLocation(),
                tarantool.getInit().getEngine(),
                tarantool.getRequestTimeout()
        );
        initializer.initialize();
        return initializer;
    }

    @Bean
    KeyValueStoragePort keyValueStoragePort(
            TarantoolBoxClient client,
            KvServiceProperties properties,
            ObjectProvider<TarantoolSpaceInitializer> initializerProvider,
            TarantoolObservability observability
    ) {
        initializerProvider.getIfAvailable();
        return new TarantoolKeyValueStorage(client, properties.getTarantool().getRequestTimeout(), observability);
    }

    @Bean("tarantoolHealthIndicator")
    TarantoolReadinessHealthIndicator tarantoolHealthIndicator(
            TarantoolBoxClient client,
            KvServiceProperties properties
    ) {
        return new TarantoolReadinessHealthIndicator(client, properties.getTarantool().getRequestTimeout());
    }
}
