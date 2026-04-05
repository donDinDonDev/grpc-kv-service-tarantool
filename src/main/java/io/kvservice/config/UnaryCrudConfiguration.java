package io.kvservice.config;

import io.kvservice.application.CountEntriesUseCase;
import io.kvservice.application.DeleteValueUseCase;
import io.kvservice.application.GetValueUseCase;
import io.kvservice.application.KeyValueValidator;
import io.kvservice.application.PutValueUseCase;
import io.kvservice.application.RangeValueUseCase;
import io.kvservice.application.storage.KeyValueStoragePort;
import io.kvservice.observability.RangeStreamMetrics;
import io.kvservice.transport.grpc.GrpcCountRequestBudgetFactory;
import io.kvservice.transport.grpc.GrpcNullableBytesMapper;
import io.kvservice.transport.grpc.GrpcStatusTranslator;
import io.kvservice.transport.grpc.GrpcUnaryRequestBudgetFactory;
import io.kvservice.transport.grpc.KvGrpcService;
import io.kvservice.transport.grpc.RangeStreamPermitLimiter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.service.GrpcService;

@Configuration(proxyBeanMethods = false)
@ConditionalOnBean(KeyValueStoragePort.class)
public class UnaryCrudConfiguration {

  @org.springframework.context.annotation.Bean
  KeyValueValidator keyValueValidator(KvServiceProperties properties) {
    return new KeyValueValidator(
        properties.getLimits().getMaxKeyBytes(), properties.getLimits().getMaxValueBytes());
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
  RangeValueUseCase rangeValueUseCase(
      KeyValueStoragePort storage, KeyValueValidator validator, KvServiceProperties properties) {
    return new RangeValueUseCase(storage, validator, properties.getRange().getBatchSize());
  }

  @org.springframework.context.annotation.Bean
  CountEntriesUseCase countEntriesUseCase(KeyValueStoragePort storage) {
    return new CountEntriesUseCase(storage);
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

  @org.springframework.context.annotation.Bean
  GrpcCountRequestBudgetFactory grpcCountRequestBudgetFactory(KvServiceProperties properties) {
    return new GrpcCountRequestBudgetFactory(properties.getDeadlines().getCount());
  }

  @org.springframework.context.annotation.Bean
  RangeStreamPermitLimiter rangeStreamPermitLimiter(KvServiceProperties properties) {
    return new RangeStreamPermitLimiter(properties.getRange().getMaxActiveStreams());
  }

  @GrpcService
  KvGrpcService kvGrpcService(
      PutValueUseCase putValueUseCase,
      GetValueUseCase getValueUseCase,
      DeleteValueUseCase deleteValueUseCase,
      RangeValueUseCase rangeValueUseCase,
      CountEntriesUseCase countEntriesUseCase,
      GrpcNullableBytesMapper valueMapper,
      GrpcUnaryRequestBudgetFactory requestBudgetFactory,
      GrpcCountRequestBudgetFactory countRequestBudgetFactory,
      GrpcStatusTranslator statusTranslator,
      RangeStreamPermitLimiter rangeStreamPermitLimiter,
      RangeStreamMetrics rangeStreamMetrics) {
    return new KvGrpcService(
        putValueUseCase,
        getValueUseCase,
        deleteValueUseCase,
        rangeValueUseCase,
        countEntriesUseCase,
        valueMapper,
        requestBudgetFactory,
        countRequestBudgetFactory,
        statusTranslator,
        rangeStreamPermitLimiter,
        rangeStreamMetrics);
  }
}
