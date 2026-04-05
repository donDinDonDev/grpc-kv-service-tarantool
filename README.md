# grpc-kv-service-tarantool

gRPC key-value сервис на Spring Boot 4 и Tarantool. Публичный контракт API находится в [`src/main/proto/kv/v1/kv_service.proto`](src/main/proto/kv/v1/kv_service.proto), локальный запуск и контейнерный сценарий опираются на [`application.yml`](src/main/resources/application.yml), [`application-local.yml`](src/main/resources/application-local.yml), [`application-container.yml`](src/main/resources/application-container.yml), [`Dockerfile`](Dockerfile), [`docker-compose.yml`](docker-compose.yml) и [`scripts/start-local-stack.sh`](scripts/start-local-stack.sh).

## Архитектура

Сервис собран как один Spring Boot runtime с явным разделением публичного transport-слоя и внутренних application/storage обязанностей:

- `transport.grpc` принимает gRPC-запросы, маппит их в use case-слой и переводит внутренние ошибки в gRPC status codes.
- `application` содержит валидацию, nullable semantics, deadline budget и правила для `Put/Get/Delete/Range/Count` без привязки к Tarantool SDK.
- `storage.tarantool` реализует доступ к `space KV`, точный `count()`, batched `range` и idempotent init через Tarantool.
- `config` связывает профили `local`/`container`, порты, лимиты, deadlines и параметры подключения.
- `observability` отвечает за structured logging, readiness/liveness, метрики, Micrometer/OpenTelemetry-compatible tracing и ограничения runtime-потока вроде контроля активных `range`-stream.

## Требования

- JDK 21
- Docker и Docker Compose
- Bash для `scripts/start-local-stack.sh`

`./mvnw clean test` поднимает реальный Tarantool через Testcontainers, поэтому для тестов нужен доступный Docker daemon.

## Быстрый старт

Самый короткий способ поднять локальный стенд:

```bash
./scripts/start-local-stack.sh
curl http://127.0.0.1:8080/actuator/health/readiness
```

Скрипт:

- при отсутствии `KV_TARANTOOL_PASSWORD` пытается сгенерировать пароль через `openssl`, а если его нет, через `uuidgen`; если обе утилиты отсутствуют, завершится с ошибкой и попросит задать пароль вручную;
- выполняет `docker compose up --build -d`;
- если в окружении есть `curl`, ждёт readiness endpoint `http://127.0.0.1:${KV_HTTP_PORT:-8080}/actuator/health/readiness`; без `curl` поднимает Compose и завершается без ожидания readiness.

Остановить стенд:

```bash
docker compose down -v
```

## Локальный запуск без контейнера приложения

Если нужен запуск Spring Boot из хоста, а Tarantool оставить в Docker:

```bash
export KV_TARANTOOL_PASSWORD=kvservice
docker compose up -d tarantool
./mvnw spring-boot:run
```

По умолчанию активен профиль `local`, поэтому приложение слушает HTTP на `8080`, gRPC на `9090` и ожидает Tarantool на `127.0.0.1:3301`. Контейнерный профиль `container` используется в `Dockerfile` и `docker-compose.yml`.

## Сборка, тесты и CI

Сборка jar:

```bash
./mvnw package
```

Полный локальный прогон тестов:

```bash
./mvnw clean test
```

Отдельный reproducible perf/load прогон:

```bash
./mvnw -Pperf verify
```

Этот профиль не подтягивается в обычный `clean test`, пишет evidence в `target/perf-reports/kvservice-performance-report.md` и описан в [`docs/performance-note.md`](docs/performance-note.md).

Отдельный bounded heavy-scale прогон:

```bash
./mvnw -Pperf-heavy verify
```

Этот workflow не входит в обычный `clean test`, не заменяет `./mvnw -Pperf verify`, а методика и caveats описаны в [`docs/performance-note.md`](docs/performance-note.md).

GitHub Actions workflow находится в [`.github/workflows/ci.yml`](.github/workflows/ci.yml) и запускает ту же реальную команду проекта:

```bash
./mvnw --batch-mode --no-transfer-progress clean test
```

Никаких декоративных шагов без сборки проекта в workflow нет.

## Docker Compose

Прямой запуск через Compose без helper-скрипта:

```bash
export KV_TARANTOOL_PASSWORD=kvservice
docker compose up --build -d
```

Что делает `docker-compose.yml`:

- поднимает `tarantool` на основе `tarantool/tarantool:3.2.1`;
- поднимает `kvservice` из текущего репозитория через `Dockerfile`;
- публикует HTTP-порт `8080` и gRPC-порт `9090`;
- требует `KV_TARANTOOL_PASSWORD` для авторизации Tarantool и приложения;
- использует профиль `container`, где host Tarantool по умолчанию равен `tarantool`.

## Контракт API

Сервис реализует пять RPC из [`src/main/proto/kv/v1/kv_service.proto`](src/main/proto/kv/v1/kv_service.proto):

| RPC | Семантика |
| --- | --- |
| `Put` | Создаёт или перезаписывает запись. `value` обязателен по семантике: нужно явно передать либо `data`, либо `null_value`. |
| `Get` | Возвращает запись по ключу. Если записи нет, вызов завершается `NOT_FOUND`. |
| `Delete` | Идемпотентно удаляет запись. Отсутствующий ключ не считается ошибкой. |
| `Range` | Server-streaming диапазон `[key_since, key_to)` в порядке возрастания ключа. |
| `Count` | Возвращает точное количество записей на момент вызова. |

### Null semantics

Контракт использует `NullableBytes` c `oneof kind { data; null_value; }`, поэтому на уровне API различаются три разных состояния:

- записи нет: `Get` возвращает `NOT_FOUND`;
- запись есть и `value = null`: успешный ответ с `record.value.null_value`;
- запись есть и `value = empty bytes`: успешный ответ с `record.value.data` длиной `0`.

Отсутствие поля `PutRequest.value` не трактуется как `null`: это `INVALID_ARGUMENT`.

### Особенности `Range`

- диапазон полузакрытый: `[key_since, key_to)`;
- сортировка идёт по ключу, по возрастанию;
- порядок сравнения ключей основан на UTF-8 байтах, без locale-aware сортировки;
- `key_since` и `key_to` проходят ту же валидацию, что и обычный `key`;
- если `key_since >= key_to`, сервис возвращает `INVALID_ARGUMENT`;
- пустой корректный диапазон возвращает пустой stream;
- сервер читает range внутренними батчами и не обещает snapshot-consistent результат при параллельных изменениях данных.

## Конфигурация

### Файлы и профили

| Файл | Назначение |
| --- | --- |
| [`src/main/resources/application.yml`](src/main/resources/application.yml) | Общие defaults: порты, health/metrics, лимиты, deadlines, параметры Tarantool. |
| [`src/main/resources/application-local.yml`](src/main/resources/application-local.yml) | Значения для запуска с профилем `local` (профиль по умолчанию). |
| [`src/main/resources/application-container.yml`](src/main/resources/application-container.yml) | Значения для профиля `container`, используемого в Dockerfile и Compose. |

### Ключевые переменные окружения

| Переменная | Default | Назначение |
| --- | --- | --- |
| `SPRING_PROFILES_ACTIVE` | `local` | Профиль запуска. Для контейнера используется `container`. |
| `KV_HTTP_PORT` | `8080` | HTTP/Actuator порт. |
| `KV_GRPC_PORT` | `9090` | gRPC порт сервиса. |
| `MANAGEMENT_SERVER_PORT` | не задан | Опциональный отдельный Actuator listener. Если задан, Actuator endpoints переезжают на этот порт, а на основном HTTP listener остаются `/livez` и `/readyz`. |
| `MANAGEMENT_SERVER_ADDRESS` | не задан | Опциональный bind address для отдельного management listener. |
| `MANAGEMENT_TRACING_EXPORT_ENABLED` | `true` | Включает tracing auto-configuration для trace propagation/export. При `false` базовый runtime, request-id и метрики продолжают работать без tracing stack. |
| `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | `0.1` | Доля request-ов, для которых создаётся sampled trace. |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | не задан | OTLP endpoint для экспорта trace-ов. Без этой настройки внешний backend не требуется. |
| `KV_TARANTOOL_HOST` | `127.0.0.1` в `local`, `tarantool` в `container` | Адрес Tarantool. |
| `KV_TARANTOOL_PORT` | `3301` | Порт Tarantool. |
| `KV_TARANTOOL_USERNAME` | `kvservice` | Пользователь Tarantool. |
| `KV_TARANTOOL_PASSWORD` | пустая строка в конфигурации; обязателен для Compose | Пароль Tarantool и приложения. |
| `KV_TARANTOOL_ENABLED` | `true` | Включает storage-слой Tarantool. |
| `KV_TARANTOOL_CONNECT_TIMEOUT` | `3s` | Таймаут установления соединения с Tarantool. |
| `KV_TARANTOOL_RECONNECT_AFTER` | `1s` | Задержка перед reconnect. |
| `KV_TARANTOOL_REQUEST_TIMEOUT` | `5s` | Таймаут запросов в Tarantool и init-скрипта. |
| `KV_TARANTOOL_INIT_ENABLED` | `true` | Автоматический idempotent init space `KV` на старте приложения. |
| `KV_TARANTOOL_ENGINE` | `vinyl` | Engine init-скрипта Tarantool. Для local/dev допускается `memtx`, но это не production default. |
| `KV_STARTUP_TIMEOUT_SECONDS` | `60` | Таймаут ожидания readiness в `scripts/start-local-stack.sh`. |

### Runtime defaults и лимиты

| Параметр | Default | Комментарий |
| --- | --- | --- |
| gRPC max request size | `4MB` | Если входящее сообщение больше, gRPC завершает вызов `RESOURCE_EXHAUSTED`. |
| gRPC max response size | `4MB` | Если ответ больше лимита, сервер завершает вызов `RESOURCE_EXHAUSTED`. |
| Max key size | `256B` | Длина считается в байтах UTF-8. Пустые ключи и control-символы запрещены. |
| Max value size | `1MiB` | Проверяется только для ненулевого payload. |
| Default unary deadline (`put/get/delete`) | `3s` | Используется, если клиент не прислал собственный deadline. |
| Default `count` deadline | `15s` | Отдельный timeout profile для потенциально более тяжёлой операции. |
| Internal `range` batch size | `256` | Текущий repo default после perf review; не часть внешнего контракта. Конкретные сравнения нужно читать по свежему generated perf report. |
| Max active `range` streams | `16` | При превышении лимита сервис возвращает `UNAVAILABLE`. |

### Health, metrics и порты

- HTTP/Actuator: `http://127.0.0.1:8080`
- gRPC: `127.0.0.1:9090`
- health endpoint: `/actuator/health`
- liveness endpoint: `/actuator/health/liveness`
- readiness endpoint: `/actuator/health/readiness`
- metrics endpoint: `/actuator/metrics`

Readiness зависит от доступности Tarantool и пространства `KV`, а liveness от состояния Tarantool не зависит. Логи пишутся в structured JSON format в stdout.

Если включён отдельный management listener через `MANAGEMENT_SERVER_PORT`, Actuator endpoints публикуются на management port, а основной HTTP listener дополнительно отдаёт `/livez` и `/readyz`, чтобы orchestration probes продолжали проверять основной listener, а не только отдельный management context. Tracing/export остаётся опциональным: starter уже включён в runtime, но auto-configuration можно отключить через `MANAGEMENT_TRACING_EXPORT_ENABLED=false`, а внешний backend нужен только если задан `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT`. OTLP metrics export по умолчанию выключен, чтобы tracing не тащил за собой отдельный metrics backend.

## Инициализация Tarantool

На старте приложения выполняется idempotent init-скрипт [`src/main/resources/tarantool/kv-init.lua`](src/main/resources/tarantool/kv-init.lua). Он:

- создаёт space `KV`, если его ещё нет;
- фиксирует поля `key: string` и `value: varbinary, nullable`;
- создаёт primary `TREE` index по `key`;
- проверяет совпадение engine уже существующего `KV` space с `KV_TARANTOOL_ENGINE`.

Ручная подготовка схемы вне репозитория не требуется.

## Реальные trade-offs v1

- Production default для `KV` space: `vinyl`. Это более честный default при неизвестном объёме данных; `memtx` оставлен как local/dev caveat, а не production-обещание.
- `count()` остаётся точным и не использует кэш или приблизительные счётчики. Цена за это: более тяжёлый latency profile по сравнению с обычными CRUD-операциями.
- `range` реализован как server-streaming RPC с внутренним батчингом. Это ограничивает потребление памяти, но не даёт snapshot semantics для всего диапазона.
- Сервис не нормализует ключи, не делает lowercase/trim и не применяет locale-aware сортировку. Поведение целиком определяется переданной UTF-8 строкой и её байтовым порядком.
