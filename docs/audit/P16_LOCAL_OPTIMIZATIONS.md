# Локальные оптимизации после принятой W4-пары

## Сохранённая W4-пара

Evidence исходного кода `b64f2be4e1c9c623009281052398adfb995c4ed8` сохранён
в [компактном JSON](evidence/P16_AUD008_W4_B64F2BE.json). Исходные JSON и логи
остаются в архиве `vm-evidence.tgz`, его SHA-256 указан в сводке.

Оба полных D2/W4 получили PASS, golden совпал, замеры приняты. При одном
bundle и `CAD_MASTER_DIAGNOSTICS=1` wave дал 125,78, pipeline — 136,74
group-ticks/s (+8,72%). Хвост Master от последней отправки принятого fragment
до последнего join составил 234,88/205,15 с. Занятость четырёх handler slots —
23,92/26,98%. W1 отложен по принятому порогу 10% для следующей пары.

## Декодирование envelope

`processPartition` декодирует и проверяет JSON, header, ключ и партицию.
Master принимает `DecodedEnvelope`, проверяет execution и научный payload.
При чужом execution в хранилище передаётся `nil` payload для продвижения
позиции. Байтовые API сохранены для существующих вызывающих функций.

Локально, Go 1.27.2, Windows amd64, i7-12650H, GOMAXPROCS=4. Три повтора
`-benchtime=200ms`, ниже median. Одна синтетическая запись около 5,7 KB,
без Kafka и SQL. Benchmark охватывает routing, декодирование, Master validation,
метрики и приём stub-хранилищем.

| Execution | До, мкс/op | После, мкс/op | Изменение времени | B/op до/после |
| --- | ---: | ---: | ---: | ---: |
| Собственный | 265,223 | 119,096 | −55,1% | 189281 / 97561 |
| Чужой | 117,985 | 51,462 | −56,4% | 91850 / 46023 |

Эти микрозамеры относятся к отдельным участкам кода. Ускорение полного D2
после локальных изменений ещё не измерено.

```sh
go test -count=1 ./internal/modules/events/delivery/resulttransport -run '^TestKafkaDecoded'
go test -run '^$' -bench '^BenchmarkKafkaEnvelopePath$' -benchmem -benchtime=200ms -count=3 ./internal/modules/events/delivery/resulttransport
```
