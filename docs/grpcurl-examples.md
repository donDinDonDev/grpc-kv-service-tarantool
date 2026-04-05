# grpcurl examples

Этот документ показывает runnable `grpcurl` команды для всех пяти RPC без server reflection. Во всех примерах используется repo-managed `.proto`:

- import path: `src/main/proto`
- proto file: `kv/v1/kv_service.proto`
- service symbol: `kv.v1.KvService`

## Требования к запуску

- команды запускаются из корня репозитория;
- локальный стенд уже поднят (`./scripts/start-local-stack.sh` или `docker compose up --build -d`);
- gRPC listener доступен на `127.0.0.1:9090`, если вы не меняли `KV_GRPC_PORT`;
- утилита `grpcurl` установлена отдельно; репозиторий не требует server reflection и не хранит отдельный checked-in descriptor set.
- JSON-представление `bytes` в `grpcurl` передаётся как base64-строка, поэтому `QQ==` ниже означает байт `A`, а `Qw==` означает байт `C`.

Во всех примерах ниже используется один и тот же `.proto` path:

```bash
grpcurl -plaintext \
  -emit-defaults \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  127.0.0.1:9090 \
  kv.v1.KvService/Count
```

## Smoke sequence

Ниже приведена короткая последовательность, которая покрывает все пять RPC и одновременно проверяет ключевые контрактные свойства.

### 1. Count на пустом стенде

```bash
grpcurl -plaintext \
  -emit-defaults \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  127.0.0.1:9090 \
  kv.v1.KvService/Count
```

Ожидаемый смысл ответа: `{"count":"0"}` на свежем локальном стенде.

### 2. Put с явным bytes payload

`Put` не допускает скрытого отсутствия `value`: нужно явно передать либо `data`, либо `nullValue`.

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"key":"demo/a","value":{"data":"QQ=="}}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Put
```

### 3. Put с явным `nullValue`

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"key":"demo/b","value":{"nullValue":{}}}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Put
```

### 4. Дополнительная запись для half-open Range

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"key":"demo/c","value":{"data":"Qw=="}}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Put
```

### 5. Get существующей записи с `null`

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"key":"demo/b"}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Get
```

Ожидаемый смысл ответа: успешный `Get`, где `record.value.nullValue` отличает существующую запись с `null` от отсутствующей записи.

### 6. Get отсутствующего ключа

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"key":"demo/missing"}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Get
```

Ожидаемый смысл ответа: `NOT_FOUND`.

### 7. Range для диапазона `[demo/a, demo/c)`

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"keySince":"demo/a","keyTo":"demo/c"}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Range
```

Ожидаемый смысл ответа:

- stream содержит `demo/a`;
- stream содержит `demo/b`;
- `demo/c` не входит, потому что верхняя граница исключающая;
- результат не нужно документировать как snapshot-consistent при параллельных изменениях.

### 8. Count после трёх `Put`

```bash
grpcurl -plaintext \
  -emit-defaults \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  127.0.0.1:9090 \
  kv.v1.KvService/Count
```

Ожидаемый смысл ответа на свежем локальном стенде: `{"count":"3"}`.

### 9. Delete и идемпотентность

Первое удаление:

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"key":"demo/b"}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Delete
```

Повторное удаление того же ключа:

```bash
grpcurl -plaintext \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  -d '{"key":"demo/b"}' \
  127.0.0.1:9090 \
  kv.v1.KvService/Delete
```

Ожидаемый смысл: оба вызова успешны; отсутствие записи не считается ошибкой.

### 10. Count после двух `Delete`

```bash
grpcurl -plaintext \
  -emit-defaults \
  -import-path src/main/proto \
  -proto kv/v1/kv_service.proto \
  127.0.0.1:9090 \
  kv.v1.KvService/Count
```

Ожидаемый смысл ответа на свежем локальном стенде: `{"count":"2"}`.

## Контрактные напоминания

- `Put` без `value` должен завершаться `INVALID_ARGUMENT`; отсутствие поля не трактуется как неявный `null`.
- `Get` по отсутствующему ключу должен возвращать `NOT_FOUND`.
- `Delete` остаётся идемпотентным.
- `Range` документируется как диапазон `[key_since, key_to)` без snapshot-consistency promise.
- `Count` возвращает exact count и может иметь более тяжёлый latency profile, чем обычные unary RPC.
