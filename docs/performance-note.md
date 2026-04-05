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

Отдельный heavy-scale workflow:

```bash
./mvnw -Pperf-heavy verify
```

Эта команда:

- запускает только heavy-scale harness из `src/test/java/io/kvservice/perf/KvServiceHeavyScaleIT.java`;
- оставляет `./mvnw clean test` и `./mvnw -Pperf verify` без изменения роли и длительности;
- пишет отдельный generated report в `target/perf-heavy-reports/kvservice-heavy-scale-report.md`;
- по умолчанию использует bounded dataset `5_000_000` записей, bulk seeding через direct Tarantool eval batches и не готовит dataset через обычный unary gRPC loop.

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

Heavy-scale workflow измеряет отдельно:

- bulk seeding large dataset до `5_000_000` записей через `scripts/perf-heavy-seed.lua`;
- exact `count()` на pre-seeded large dataset;
- длинный `range` по большому окну с TTFI и stream completion evidence;
- representative unary probes против большого key-space;
- bounded resource-aware evidence через phase snapshots и peak JVM/range-stream metrics.

## Heavy-scale decision rule

- Heavy workflow не ретюнит defaults по single-run winner.
- Любой candidate для `range.batch-size`, `range.max-active-streams`, deadlines или `KV_TARANTOOL_REQUEST_TIMEOUT` должен подтверждаться серией минимум из 3 controlled heavy runs в одном canonical measurement mode.
- Для смены default недостаточно локального выигрыша только по одному числу: нужен согласованный результат по throughput, latency/TTFI, guardrail behavior и bounded resource pressure.
- Если evidence неоднозначен или noisy, defaults остаются без изменений.

## Текущий verdict по defaults

- `range.batch-size` снижен с `512` до `256` в текущем repo default после perf review, но это не должно читаться как универсальный или стабильно воспроизводимый выигрыш по всем метрикам и на любой машине. Canonical perf workflow используется как проверка существующего runtime knob в одном measurement mode; authoritative source для конкретных latency/throughput сравнений остаётся свежий generated report в `target/perf-reports`.
- `range.max-active-streams` оставлен `16` как консервативный guardrail по concurrent streams. Текущий single-host perf workflow не даёт оснований публично объявлять `32` устойчиво лучшим default-ом, поэтому лимит не повышается без дополнительного resource evidence и более стабильной серии прогонов.
- `deadlines` (`3s` unary, `15s` count) и `KV_TARANTOOL_REQUEST_TIMEOUT=5s` оставлены без изменений: measured unary/range/count latencies в canonical mode не показывают необходимости пересматривать эти guardrails в рамках текущего single-host evidence.
- Heavy-scale workflow добавляет stronger scale evidence поверх `S10`, но сам по себе не заменяет local baseline и не даёт права на noisy retuning: пока нет series-based comparison в одном measurement mode, defaults считаются подтверждённо неизменёнными, а не "выигравшими" по single rerun.

## Как читать результаты

- Perf harness не вводит жёсткие throughput thresholds и не превращается в обязательный PR gate.
- Отчёт нужен для evidence-backed review текущих defaults и честного сравнения в одной и той же среде.
- Если измерения не дают сопоставимого выигрыша в одном canonical mode, runtime defaults оставляются без изменений.
- Если конкретный rerun даёт другие числа, authoritative источником считается именно свежий generated report этого запуска, а не ранее написанная prose-формулировка.
- Heavy-scale report и local baseline report нельзя смешивать в одну таблицу или трактовать как один и тот же measurement layer: heavy workflow дополняет `S10`, а не заменяет его.
- Если current default guardrail для тяжёлого `count()` не даёт снять exact completion evidence на large dataset, heavy workflow сначала фиксирует это как отдельный default probe, а completion-run выполняется только на явно помеченном candidate timeout profile. Такой candidate нужен для evidence collection и сам по себе не означает смену repo default.

## Caveats

- Все числа относятся к конкретной машине и моменту запуска.
- Клиент, сервис и Tarantool делят один host, поэтому результаты нельзя трактовать как SLA или capacity promise.
- Для user-facing perf summary нужно опираться на generated report, а не на неподтверждённые extrapolation claims.
- Для быстрых локальных прогонов допустимо уменьшать heavy dataset через `-Dkvservice.perf-heavy.dataset-size=...`, но authoritative scale evidence для этого workflow начинается только там, где реально выполнен large-dataset run, а не synthetic extrapolation из малого набора.
