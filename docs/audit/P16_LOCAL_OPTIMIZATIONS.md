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
позиции. Существующие байтовые API декодируют вход перед вызовом Master.

Локально, Go 1.27.2, Windows amd64, i7-12650H, GOMAXPROCS=4. Три повтора
`-benchtime=200ms`, ниже median. Одна синтетическая запись около 5,7 KB,
без Kafka и SQL. Benchmark охватывает routing, декодирование, Master validation,
метрики и приём stub-хранилищем.

| Execution | До, мкс/op | После, мкс/op | Изменение времени | B/op до/после |
| --- | ---: | ---: | ---: | ---: |
| Собственный | 265,223 | 119,096 | −55,1% | 189281 / 97561 |
| Чужой | 117,985 | 51,462 | −56,4% | 91850 / 46023 |

Эти микрозамеры относятся к отдельным участкам кода. Эффект на полный D2
после локальных изменений требует отдельного замера.

```sh
go test -count=1 ./internal/modules/events/delivery/resulttransport -run '^TestKafkaDecoded'
go test -run '^$' -bench '^BenchmarkKafkaEnvelopePath$' -benchmem -benchtime=200ms -count=3 ./internal/modules/events/delivery/resulttransport
```

## AnnotateOutside

Общие `Available` и `Anomalous` суммируются один раз. Для каждой группы
вычитается её собственный вклад. Порядок `outside_groups` следует входному
срезу. Формирование этих списков по-прежнему требует квадратичного объёма
работы и памяти при увеличении числа групп.

Для сравнения расчёта в одном benchmark-бинарнике сохранена прежняя реализация
`Before`. `After` вызывает новую функцию. Сериализация измеряется отдельно,
после аннотации, по одному `GroupResult` на группу. Каждая синтетическая группа
содержит `Evidence.Raw` и минимальный Raw с IF-счётчиками, без массивов показаний
18 узлов. Один op охватывает весь набор групп. Median трёх локальных повторов
в микросекундах приведена ниже.

| Групп | Расчёт до | Расчёт после | JSON до | JSON после |
| --- | ---: | ---: | ---: | ---: |
| 16 | 3,136 | 3,102 | 277,222 | 279,694 |
| 64 | 40,863 | 42,027 | 1508,986 | 1545,478 |
| 200 | 1294,278 | 1155,623 | 8654,054 | 8455,350 |

В этих коротких замерах заметен разброс. Для 64 групп median расчёта выросла,
для 200 снизилась. Результаты всех повторов
доступны в [JSON локальных бенчмарков](evidence/P16_LOCAL_BENCHMARKS.json).

Регрессия сравнивает значения и байты JSON старого и нового расчётов, включая
пустой и одиночный наборы, отсутствующие наблюдения, повторяющиеся имена групп,
`null`, повторную аннотацию и `Evidence.Raw`. Отдельная read-only проверка
сохранённых файлов с pinned SHA подтвердила совпадение 46 080 Raw и 1 284
Evidence.Raw с принятым golden. Эта проверка читает готовые результаты.

```sh
go test -count=1 ./internal/modules/detection/group -run '^TestAnnotateOutside'
go test -run '^$' -bench '^Benchmark(AnnotateOutside|OutsideSerialization)$' -benchmem -benchtime=200ms -count=3 ./internal/modules/detection/group
# OUTSIDE_GOLDEN_DIR должен содержать исходные pinned raw.ndjson и evidence.ndjson.
go test -count=1 -v ./internal/modules/detection/group -run '^TestAnnotateOutsideSavedGolden$'
go vet ./internal/modules/detection/group ./internal/modules/events/domain ./internal/modules/events/usecase ./internal/modules/events/delivery/resulttransport ./internal/app/master
```
