package io.kvservice.config;

import io.grpc.ServerBuilder;
import io.kvservice.observability.GrpcObservabilityInterceptor;
import io.kvservice.observability.GrpcResponseSizeLimitInterceptor;
import io.kvservice.observability.RangeStreamMetrics;
import io.kvservice.observability.TarantoolObservability;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.grpc.server.ServerBuilderCustomizer;

@Configuration(proxyBeanMethods = false)
public class ObservabilityConfiguration {

    @Bean
    TarantoolObservability tarantoolObservability(MeterRegistry meterRegistry) {
        return new TarantoolObservability(meterRegistry);
    }

    @Bean
    RangeStreamMetrics rangeStreamMetrics(MeterRegistry meterRegistry) {
        return new RangeStreamMetrics(meterRegistry);
    }

    @Bean
    @GlobalServerInterceptor
    @Order(0)
    GrpcObservabilityInterceptor grpcObservabilityInterceptor(MeterRegistry meterRegistry) {
        return new GrpcObservabilityInterceptor(meterRegistry);
    }

    @Bean
    @GlobalServerInterceptor
    @Order(1)
    GrpcResponseSizeLimitInterceptor grpcResponseSizeLimitInterceptor(KvServiceProperties properties) {
        return new GrpcResponseSizeLimitInterceptor(properties.getGrpc().getMaxResponseBytes().toBytes());
    }

    @Bean
    @SuppressWarnings({ "rawtypes", "unchecked" })
    ServerBuilderCustomizer grpcRequestSizeLimitCustomizer(KvServiceProperties properties) {
        int maxRequestBytes = Math.toIntExact(properties.getGrpc().getMaxRequestBytes().toBytes());
        return new ServerBuilderCustomizer() {
            @Override
            public void customize(ServerBuilder serverBuilder) {
                serverBuilder.maxInboundMessageSize(maxRequestBytes);
            }
        };
    }

}
