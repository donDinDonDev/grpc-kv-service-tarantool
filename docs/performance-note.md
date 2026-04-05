# Performance note

## Назначение

Этот репозиторий содержит отдельный reproducible perf/load workflow для service-level измерений через реальный Spring Boot + gRPC + Tarantool путь. Он не встроен в обычный correctness gate и не подменяет `./mvnw clean test`.

## Команда

```bash
./mvnw -Pperf verify
```

Команда:

- запускает только perf harness из `src/test/java/io/kvservice/perf`;
- использует реальный Tarantool через Testcontainers;
- пишет captured evidence в `target/perf-reports/kvservice-performance-report.md`.

## Canonical measurement mode

Для baseline-измерений используется один канонический режим:

- `management.tracing.export.enabled=false`;
- внешний OTLP backend не требуется;
- стандартные runtime logs и metrics остаются включены.

Результаты из других tracing/export режимов нельзя сравнивать с этим baseline без явной пометки.

## Что измеряется

- массовые `put/get/delete`;
- конкурентная unary-нагрузка на реальном gRPC path;
- длинный `range`;
- exact `count()`;
- review входов для `range.batch-size`, `range.max-active-streams` и текущих timeout/deadline defaults.

## Текущий verdict по defaults

- `range.batch-size` снижен с `512` до `256` в текущем repo default после perf review, но это не должно читаться как универсальный или стабильно воспроизводимый выигрыш по всем метрикам и на любой машине. Canonical perf workflow используется как проверка существующего runtime knob в одном measurement mode; authoritative source для конкретных latency/throughput сравнений остаётся свежий generated report в `target/perf-reports`.
- `range.max-active-streams` оставлен `16` как консервативный guardrail по concurrent streams. Текущий single-host perf workflow не даёт оснований публично объявлять `32` устойчиво лучшим default-ом, поэтому лимит не повышается без дополнительного resource evidence и более стабильной серии прогонов.
- `deadlines` (`3s` unary, `15s` count) и `KV_TARANTOOL_REQUEST_TIMEOUT=5s` оставлены без изменений: measured unary/range/count latencies в canonical mode не показывают необходимости пересматривать эти guardrails в рамках текущего single-host evidence.

## Как читать результаты

- Perf harness не вводит жёсткие throughput thresholds и не превращается в обязательный PR gate.
- Отчёт нужен для evidence-backed review текущих defaults и честного сравнения в одной и той же среде.
- Если измерения не дают сопоставимого выигрыша в одном canonical mode, runtime defaults оставляются без изменений.
- Если конкретный rerun даёт другие числа, authoritative источником считается именно свежий generated report этого запуска, а не ранее написанная prose-формулировка.

## Caveats

- Все числа относятся к конкретной машине и моменту запуска.
- Клиент, сервис и Tarantool делят один host, поэтому результаты нельзя трактовать как SLA или capacity promise.
- Для user-facing perf summary нужно опираться на generated report, а не на неподтверждённые extrapolation claims.
