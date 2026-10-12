# ДЗ №2: CSV → Kafka → ClickHouse

Ветка [ht-2](https://github.com/klyuchnikova/real-time-fraud-detection/tree/ht-2). Отдельный Python-скрипт отправляет CSV в Kafka, ClickHouse Kafka Engine и materialized views сохраняют транзакции и рассчитывают максимум по каждому штату. Подготовлены итоговый SQL, результат для полного test.csv, тесты реального DDL и измерения оптимизации.

[План](PLAN.md) · [Условие](TASK.md) · [Результат CSV](results/max_transaction_by_state.csv) · [Проверка результата](results/max_transaction_by_state.metadata.json)

## Первый запуск

Нужны Git, Docker Engine с Linux-контейнерами и Compose v2. На Windows используйте запущенный Docker Desktop/WSL 2. Проверено с Docker 27.5.1, Compose 2.32.4, 4 CPU и 6 ГБ памяти Docker. Kafka ограничена 1.5 ГБ, ClickHouse — 2 ГБ. Python и ClickHouse отдельно устанавливать не нужно.

~~~bash
git clone --branch ht-2 https://github.com/klyuchnikova/real-time-fraud-detection.git
cd real-time-fraud-detection
docker version
docker compose version
~~~

Создайте конфигурацию. PowerShell:

~~~powershell
Copy-Item .env.example .env
~~~

Bash:

~~~bash
cp .env.example .env
~~~

Если .env уже существует, используйте его. Пример содержит пароль локального учебного окружения. После первого старта пароль должен соответствовать пользователю в сохранённом томе ClickHouse.

~~~bash
docker compose up -d
docker compose build tools
docker compose ps -a
docker compose run --rm tools python -m scripts.check_environment
~~~

Kafka и ClickHouse должны быть healthy; kafka-volume-init, kafka-init и clickhouse-init — Exited (0). Compose ожидает готовность сервисов и применение DDL перед tools. В проверенной версии Compose up --wait считает завершившийся init ошибкой даже при коде 0, поэтому используем up -d и проверку через tools.

Compose-проект real-time-fraud-detection-ht2 имеет собственные тома и локальные порты 9094/8123. Для других портов добавьте KAFKA_HOST_PORT и CLICKHOUSE_HTTP_PORT в .env. Закреплены apache/kafka:4.3.1, clickhouse/clickhouse-server:26.8.20.9, Python 3.12.12 и [requirements.txt](requirements.txt).

## Демонстрация без скачивания данных

[examples/analytics_demo.csv](examples/analytics_demo.csv) содержит 9 транзакций и 5 регионов, равные и отрицательные суммы, CSV-кавычки/запятые.

~~~bash
docker compose run --rm tools python -m scripts.load_csv_to_kafka examples/analytics_demo.csv --manifest reports/demo_manifest.json
docker compose run --rm tools python -m scripts.export_results --manifest reports/demo_manifest.json --output results/demo_max_transaction_by_state.csv
~~~

Ожидается status=success, selected_rows=confirmed_messages=9, verified_unique_records=9, regions=5. Итог показан в экспортированном CSV. Для NY две операции имеют сумму 87.80; победитель определяется ID.

Чтение CSV вынесено из прежнего Streamlit-загрузчика в [common/schema.py](common/schema.py). CLI использует общий контракт и добавляет проверку типов, идентификаторы и подтверждение доставки. Прежний интерфейс также использует этот reader.

## Полный датасет

Скачайте данные по [ссылке задания](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394) соревнования teta-ml-1-2025. Положите test.csv в teta-ml-1-2025/test.csv. Каталог подключён к tools только для чтения: уже распакованные файлы перекладывать не требуется. Источники исключены из Git и Docker build context.

~~~bash
docker compose run --rm tools python -m scripts.load_csv_to_kafka teta-ml-1-2025/test.csv
docker compose run --rm tools python -m scripts.export_results
~~~

Producer проверяет CSV, отправляет записи и сохраняет reports/load_manifest.json. Экспорт ожидает загрузку, сравнивает **все поля каждой уникальной записи** с независимым CSV-эталоном, сравнивает оба SQL и пишет results/max_transaction_by_state.csv через CSVWithNames. CSV создаётся Python, без зависимости от кодировки shell-redirection.

Приложенный результат: 262 144 записи, 51 регион (50 штатов и DC). SHA-256 test.csv: 4129db5d9079f907d9a38aefc170f5e105a650a8273457aaae02a1f735516eac. [Профиль данных](reports/dataset_profile.json) также содержит сведения о train: 786 431 строка и target. Train поддерживается отдельной загрузкой; sample_submition.csv — шаблон соревнования и не принимается загрузчиком транзакций.

Для небольшой части test.csv:

~~~bash
docker compose run --rm tools python -m scripts.load_csv_to_kafka teta-ml-1-2025/test.csv --limit 1000 --manifest reports/prefix_manifest.json
docker compose run --rm tools python -m scripts.export_results --manifest reports/prefix_manifest.json --output results/prefix_max.csv
~~~

dataset_id учитывает SHA-256 файла и число выбранных строк. Demo, prefix, train и полный test изолированы. Повтор той же выборки получает прежние ID. Загрузчик делает проход проверки, проход отправки и дополнительную проверку хеша файла; память не растёт с размером CSV. При ошибке доставки manifest получает failed и экспорт запрещён.

## DDL и запрос

Файлы [sql/clickhouse](sql/clickhouse):

- 001_baseline.sql — контрольная MergeTree для benchmark.
- 002_optimized.sql — самостоятельный итоговый DDL базы, Kafka Engine, MergeTree, AggregatingMergeTree и двух MV. Применяется автоматически clickhouse-init.
- 003_max_transaction_by_state_raw.sql — максимум непосредственно по транзакциям.
- 004_max_transaction_by_state.sql — итоговый запрос через argMaxMerge.

Ищется категория **отдельной самой крупной транзакции**, а не категория с наибольшей суммой всех операций. Сравнение по (amount, transaction_id): при равной сумме выбираем лексикографически больший ID. ID — SHA-256 строки dataset_id:row_index. Оба SQL используют одинаковый порядок и возвращают us_state, cat_id, max_amount, transaction_id, сортируя по штату.

SQL принимает database:Identifier и dataset_id:String; CLI подставляет их из manifest. Для запуска в clickhouse-client передайте --param_database=fraud_analytics и --param_dataset_id=<dataset_id> вместе с содержимым SQL. Воспроизводимый экспорт выполняется командой выше.

amount хранится как Decimal64(2), post_code — String с ведущими нулями. Время источника не имеет timezone: сохраняется исходное время с технической интерпретацией UTC, без пересчёта по штату. Сохранены все 17 исходных полей, target — Nullable(UInt8), для test это NULL. Дополнительно: row_index, ID набора/транзакции и Kafka topic/partition/offset.

Kafka Engine имеет семантику at least once: сырые записи могут дублироваться. Повторы одной операции не меняют max; argMaxMerge объединяет состояния при чтении до фоновых слияний. Это не exactly once и не решение для сумм/счётчиков. IF NOT EXISTS не мигрирует существующую схему: при изменении DDL потребуется migration или пересоздание учебного окружения с replay. MV получает новые вставки; для уже существующих данных нужен backfill.

## Оптимизация

MergeTree сортируется по (dataset_id, us_state, transaction_time, transaction_id). LowCardinality используется для штатов, категорий, пола и Kafka topic; ZSTD — для сжатия, Delta — для времени, индекса и offset. Партиции, TTL и skipping indexes не добавлены: запрос ищет максимум по всем данным выбранного набора.

Главное ускорение даёт state_max: MV поддерживает argMaxState по (dataset_id, us_state), SQL объединяет его через argMaxMerge. Цена — дополнительная работа при вставке и отдельная таблица. Витрина предназначена для целевого запроса; другая аналитика использует сырые данные.

~~~bash
docker compose run --rm tools python -m scripts.benchmark_clickhouse --merge
~~~

Benchmark очищает только контрольную transactions_baseline и копирует выбранный dataset. --merge явно выполняет OPTIMIZE FINAL для трёх таблиц перед измерением; корректность без слияний проверяется отдельным тестом. После одного прогрева выполняются 7 запусков каждого варианта в меняющемся порядке, use_query_cache=0, max_threads=2; кеш файлов остаётся прогретым. Метрики читаются из system.query_log, размеры — из system.parts. Отчёты перезаписываются.

Измерение на полном test.csv:

| Вариант | Медиана, сервер | Строк прочитано | Байтов прочитано | Память запроса |
| --- | ---: | ---: | ---: | ---: |
| baseline | 164 мс | 262 144 | 42 080 652 | 9 497 064 |
| optimized raw | 164 мс | 262 144 | 37 748 736 | 7 538 579 |
| aggregate | 6 мс | 56 | 11 886 | 4 509 707 |

Размер bytes_on_disk: baseline — 42 761 506 байт, optimized raw — 20 799 695, агрегаты — 6 260. Суммарное оптимизированное хранение — примерно 49% контрольного. Raw/aggregate дополнительно содержали 9 demo-строк и 5 demo-регионов; baseline — только test. Поэтому агрегат прочитал 56 состояний, а вернул 51. Кодеки уменьшили размер и чтение, но не изменили медиану времени этого запроса. Время зависит от машины и нагрузки.

Подробности: [benchmark.csv](reports/benchmark.csv), [benchmark_summary.json](reports/benchmark_summary.json), [storage.csv](reports/storage.csv). Benchmark временно хранит дополнительную копию исходных записей.

## Тесты и рестарт

~~~bash
docker compose run --rm tools python -m unittest discover -s tests -p test_csv_loader.py -v
docker compose run --rm tools python -m unittest discover -s tests/integration -v
~~~

9 тестов проверяют CSV/BOM/кавычки, target, Decimal, типы, ID, acknowledgements и timeout очереди. 4 интеграционных теста выполняют настоящий DDL, проверяют кодеки/движки, повторное применение без потери данных, ошибочный DDL, блоки до слияний, равные суммы, новый максимум, изоляцию, Kafka replay, все поля и экспорт. Ошибочный JSON проверяется через system.kafka_consumers. Каждый тест использует случайную базу/топик/group и удаляет только их. Старые ML-тесты относятся к hw1 и требуют его dependencies.

~~~bash
docker compose restart kafka clickhouse
docker compose run --rm tools python -m scripts.export_results --output results/restart_check.csv
~~~

После рестарта проверены все 262 144 записи и 51 регион; SHA-256 результата совпал с первоначальным. При replay raw_rows может вырасти, а число уникальных транзакций и максимум остаются прежними.

## Диагностика и остановка

~~~bash
docker compose ps -a
docker compose logs --tail=100 kafka clickhouse kafka-init clickhouse-init
docker compose down
~~~

down сохраняет тома. docker compose down -v удаляет **данные именно этого Compose-проекта**, после чего CSV надо загрузить заново. При проблеме проверьте Engine, память/порты, пароль и логи init. Ошибочные сообщения не пропускаются молча: kafka_skip_broken_messages=0; ожидание загрузки ограничено timeout и выводит диагностику consumer.

Проверен запуск из чистой локальной копии коммита a2f344c с новыми томами: сборка, environment check, demo producer/export и интеграционные тесты прошли. Датасет и .env из рабочего проекта не копировались. Отчёт — [reports/validation.json](reports/validation.json). CSV сохраняются в Git без преобразования переводов строк, чтобы не менять хеши и ID.

## Прошлое задание

Код скоринга, PostgreSQL и Streamlit сохранён. [README.hw1.md](README.hw1.md), [TASK.hw1.md](TASK.hw1.md). Запуск: docker compose -p real-time-fraud-detection-hw1 -f docker-compose.hw1.yml up -d --build. Hw1 и hw2 используют Kafka-порт 9094: сначала остановите hw2, если нужен hw1.

Документация: [Kafka Engine](https://clickhouse.com/docs/engines/table-engines/integrations/kafka), [AggregatingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree), [argMax](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/argmax), [кодеки](https://clickhouse.com/docs/sql-reference/statements/create/table#column-compression-codecs).
