# P16. Recovery и короткая серия W1/W4

Исторические прогоны на ВМ и SQL-опыт выполнены на `fd230a0aa4c0918f996ef670270f05fa459d0464`.
Recovery-ветка уже включает Worker output identity pin (`040c5b8`), ограничения
для неопределённого Kafka Produce ACK и PostgreSQL rollback (`5472ba5`) и
восстановление Master с durable SQL offset после временного сбоя вспомогательного
Kafka commit (`a5e876e`). Метрики Master и их регрессии в `07de7cf` и `de05cba`.

## Recovery на ВМ

Изолированные Kafka 4.1.2 и PostgreSQL 17.12. **PASS без SKIP** в трёх верхнеуровневых
проверках `kafka-payload-ack` и шести `kafka-recovery`. Закрыты restart и identity pin
в sync/async Worker, lost/unknown ACK, auxiliary commit, порядок и восстановление
durable offset, rebalance во время SQL wave и фактический payload около 4 MiB.
Подтверждения в `kafka-recovery.jsonl`, `kafka-payload-ack.jsonl` и `test-status.tsv`.

## W1/W4 на том же стенде

Три чередующиеся пары на 288 синтетических timestamps. Зафиксированы 16 партиций,
Master concurrency 4, Kafka poll batch 16 и `CAD_MASTER_DIAGNOSTICS=1`.

| Прогон | Group-ticks/s | Пик backlog, записей | Хвост Master, с |
| --- | ---: | ---: | ---: |
| 01 W1 | 47,57 | 282 | 22,87 |
| 02 W4 | 62,95 | 3334 | 51,40 |
| 03 W1 | 51,07 | 270 | 20,23 |
| 04 W4 | 66,26 | 3452 | 51,14 |
| 05 W1 | 51,75 | 255 | 20,65 |
| 06 W4 | 67,20 | 3228 | 49,37 |

Медиана W4 — **66,26 против 51,07 group-ticks/s** у W1 (+29,8%). Хвост Master
измерен от наблюдения 16 finalized checkpoints и нулевого pending до подтверждения
последнего join. Историческое отставание W4 на Summit D2 остаётся открытым.

Во всех шести прогонах получены 288 joins, 4608 финальных результатов, 784 parent
events и 16 finalized checkpoints при нулевых pending и duplicates. Для всех
16 партиций относительные offsets совпали (4560 входных и 337 выходных записей
на партицию). Финальные durable SQL offsets совпали с Kafka end offsets.

| SHA-256 | Digest |
| --- | --- |
| Результаты | `4fb218c4444c724f80656e3fb67b36483b38b7a8d510144d1f384c324a05067f` |
| Parent events | `1c9290edf0d1a403a429964e182818ef7b1c4c72719ad2d52d685fcb9c04e27d` |

Digest сверяются внутри серии. При повторе временный путь источника входит в
хеш сгенерированного профиля и меняет `RunID`, `MessageID` и digest.

Исходный `go test` — **FAIL** (exit 1, все шесть подпрогонов). Harness отвергал
Worker exit 130 после штатного SIGTERM, когда последняя запись `Worker stopped`
содержала `Kafka fetch /-1: context canceled`. Отдельный аудит логов и результатов
дал **PASS** для 6/6 подпрогонов. `series.jsonl` сохраняет исходный FAIL,
`shutdown-audit.json` — вердикт перепроверки, `series-summary.json` — метрики.

### Проверка интеграционной ветки

На отдельном WSL с Kafka 4.1.2 и PostgreSQL 16.15 прошли все девять
recovery и payload-тестов без SKIP, а также `TestP16ShutdownClassification`.
Полная короткая серия завершилась с **FAIL**. Прошли 5 из 6 подпрогонов,
в том числе с терминальной записью `Kafka fetch /-1: context canceled`.
В `05-w1` Worker вышел с кодом 1 до SIGTERM. В логе отмечены повторы с
`stale or expired owner` (`SQLSTATE 55000`), затем timeout обработки
Kafka record (`context deadline exceeded`). Digest и финальные offsets
для `05-w1` не получены. Остальные пять прогонов подтвердили 288 joins,
4608 результатов, 784 parent events, 16 финальных checkpoints и совпадение
digest и offsets внутри серии. Этот прогон проверял классификацию остановки.

### Ручной запуск одной пары с исправленным harness

На Linux подготовить отдельный Kafka broker и отдельный PostgreSQL cluster.
Тест пишет в `telemetry.raw` и `group.results`, создаёт и удаляет свои базы.
Передать адреса **только** тестовых сервисов через `P07_TEST_KAFKA_BROKERS` и
`P07_TEST_DATABASE_URL`. Из корня текущего checkout выполнить:

```sh
export P16_ROOT="$(mktemp -d)"
export P16_SERIES_DIR="$P16_ROOT/series"
mkdir "$P16_ROOT/bin" "$P16_SERIES_DIR"
go build -o "$P16_ROOT/bin/" ./cmd/master ./cmd/worker
P07_TEST_KAFKA_ISOLATED=1 P16_PG_OK=1 P16_KAFKA_OK=1 GOMAXPROCS=4 \
  go test -json -count=1 -timeout=8m ./internal/app/worker \
    -run '^TestP16ShortKafkaSeries$/^(01-w1|02-w4)$'
```

`TestP16ShutdownClassification` выполняется без Kafka/PostgreSQL. Для exit 130
проверяются успешный SIGTERM от harness, роль Worker, конечная запись отмены
и отсутствие дополнительных ошибок в логе. Timeout завершает подпрогон ошибкой.

## AUD-006. Решение по SQL final join

На изолированном PostgreSQL 17.5 с тем же исходным кодом batch 4 сократил
round-trips с 23 до 11. Сводная median/p95 составила 65,28/89,68 мс против
68,35/113,83 мс у baseline, а в третьей серии выросла относительно baseline на
1,8%/35,1%. У batch 16 median/p95 выросла до 80,42/140,40 мс. Регрессии
прототипа **PASS**, digest и durable offsets совпали. Требование устойчивого
A/B по всем трём сериям не выполнено. SQL batching отклонён до нового A/B.

## Сверка с сохранёнными артефактами

Для артефактов ВМ имена выше относятся к архиву `p16-recovery-diag-9YovoW/artifacts.tgz`.
Для SQL-опыта исходные `summary.json` и `regression.log` лежат в `aud-006-local/raw/`.

| Артефакт | SHA-256 |
| --- | --- |
| `artifacts.tgz` | `94022c8e8254976cfa1cb7259bf649573b54a898a607e20ce64ca78120688439` |
| `summary.json` | `660ca458f6814b0a4e03f15c5deca33f2613568314340b652d0a9b8ce31e8a64` |
| `regression.log` | `61e3f6cde6faaf39e69417e850287f74814bded2a66d063c931048acc262d868` |
