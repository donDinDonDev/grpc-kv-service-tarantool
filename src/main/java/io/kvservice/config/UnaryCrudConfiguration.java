package io.kvservice.config;

import io.kvservice.application.DeleteValueUseCase;
import io.kvservice.application.GetValueUseCase;
import io.kvservice.application.KeyValueValidator;
import io.kvservice.application.PutValueUseCase;
import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.transport.grpc.GrpcNullableBytesMapper;
import io.kvservice.transport.grpc.GrpcStatusTranslator;
import io.kvservice.transport.grpc.GrpcUnaryRequestBudgetFactory;
import io.kvservice.transport.grpc.KvGrpcService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.service.GrpcService;

@Configuration(proxyBeanMethods = false)
@ConditionalOnBean(KeyValueStoragePort.class)
public class UnaryCrudConfiguration {

    @org.springframework.context.annotation.Bean
    KeyValueValidator keyValueValidator(KvServiceProperties properties) {
        return new KeyValueValidator(
                properties.getLimits().getMaxKeyBytes(),
                properties.getLimits().getMaxValueBytes()
        );
    }

    @org.springframework.context.annotation.Bean
    PutValueUseCase putValueUseCase(KeyValueStoragePort storage, KeyValueValidator validator) {
        return new PutValueUseCase(storage, validator);
    }

    @org.springframework.context.annotation.Bean
    GetValueUseCase getValueUseCase(KeyValueStoragePort storage, KeyValueValidator validator) {
        return new GetValueUseCase(storage, validator);
    }

    @org.springframework.context.annotation.Bean
    DeleteValueUseCase deleteValueUseCase(KeyValueStoragePort storage, KeyValueValidator validator) {
        return new DeleteValueUseCase(storage, validator);
    }

    @org.springframework.context.annotation.Bean
    GrpcNullableBytesMapper grpcNullableBytesMapper() {
        return new GrpcNullableBytesMapper();
    }

    @org.springframework.context.annotation.Bean
    GrpcStatusTranslator grpcStatusTranslator() {
        return new GrpcStatusTranslator();
    }

    @org.springframework.context.annotation.Bean
    GrpcUnaryRequestBudgetFactory grpcUnaryRequestBudgetFactory(KvServiceProperties properties) {
        return new GrpcUnaryRequestBudgetFactory(properties.getDeadlines().getUnary());
    }

    @GrpcService
    KvGrpcService kvGrpcService(
            PutValueUseCase putValueUseCase,
            GetValueUseCase getValueUseCase,
            DeleteValueUseCase deleteValueUseCase,
            GrpcNullableBytesMapper valueMapper,
            GrpcUnaryRequestBudgetFactory requestBudgetFactory,
            GrpcStatusTranslator statusTranslator
    ) {
        return new KvGrpcService(
                putValueUseCase,
                getValueUseCase,
                deleteValueUseCase,
                valueMapper,
                requestBudgetFactory,
                statusTranslator
        );
    }
}
