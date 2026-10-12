# План ДЗ №2: CSV → Kafka → ClickHouse и оптимизация хранения

Дата подготовки: 12 октября 2026 года. Ветка: `ht-2`.

Реализация producer, DDL, SQL, экспорта и benchmark выполнена; ниже сохранены решения и критерии проверки. Актуальные команды и измерения находятся в README.md. Цель — законченный вариант на 8–10 баллов с воспроизводимым запуском и измеренным эффектом оптимизации.

## Выполнено 12.10.2026: данные и окружение

- CSV проверены полностью: train — 786 431 строка, test — 262 144; 14 категорий, 51 регион с DC, пропусков нет. Суммы до двух десятичных знаков, время без timezone.
- Пользователь подтвердила скачивание по ссылке задания; это тот же датасет, что в hw1. Схема и статистики также совпали с сохранённым артефактом. Файлы используются на месте, без преобразования.
- Отчёт с хешами: [reports/dataset_profile.json](reports/dataset_profile.json). Рабочий исходный файл hw2 — `teta-ml-1-2025/test.csv`; train пока отдельно, без объединения.
- Docker Desktop запущен, Linux Engine доступен: 4 CPU, около 6 ГБ памяти Docker. Есть чужой PostgreSQL; его настройки/данные не изменялись.
- Kafka `4.3.1` и ClickHouse `26.8.20.9` подняты в проекте `real-time-fraud-detection-ht2`, оба healthy. Init-контейнеры завершились с кодом 0.
- Собран tools на Python `3.12.12`; runtime-пакеты и транзитивные зависимости закреплены в requirements. Отдельная системная установка ClickHouse/Python-пакетов не нужна.
- Проверены Decimal, DDL с LowCardinality/кодеком времени, Kafka producer/consumer и **Kafka Engine → MV → MergeTree**. Временные ресурсы удалены. Отчёт: [reports/environment_check.json](reports/environment_check.json).
- Исходные CSV и локальный пароль исключены из Git; исходные данные исключены из build context.
- Сохранены `docker-compose.hw1.yml` и `README.hw1.md`; новые команды окружения — в README.
- Полный test.csv загружен: 262 144 уникальные записи, все исходные поля проверены независимым эталоном. Итоговый CSV: 51 регион. Пройдены 9 unit и 4 integration теста; рестарт сохранил побайтово идентичный результат. Benchmark: 164 мс baseline/raw против 6 мс aggregate; хранение 42.76 МБ против 20.81 МБ вместе с витриной.

## 1. Исходное состояние и границы задания

- Репозиторий: [klyuchnikova/real-time-fraud-detection](https://github.com/klyuchnikova/real-time-fraud-detection), локально `D:\E\Copy\PyCharm\real-time-fraud-detection`.
- Ветка `ht-2` создана от `7b66f07`. Прошлое задание сохранено в истории Git.
- Есть Kafka/KRaft, CatBoost, скоринг, PostgreSQL, Streamlit, Docker Compose и проверки прошлого задания. Первоначально ClickHouse отсутствовал; теперь основное Compose-окружение hw2 содержит Kafka и ClickHouse.
- `sql/001_transaction_scores.sql` — DDL PostgreSQL. Нужен отдельный DDL ClickHouse. Правильное название — **DDL** (Data Definition Language), а не DLL.
- В старом CSV есть `us_state`, `cat_id`, `amount`, `transaction_time`. Схема скачанного набора проверена: 17 полей test и 18 полей train (включая target), совпадает с hw1. Источник скачивания по ссылке задания подтверждён пользователем; CSV используются без изменения.
- Пункты 2 и 3 задания дублируются: готовим основной запрос, контрольный вариант до оптимизации и итоговый CSV.
- Трактовка: для каждого представленного в данных штата найти категорию **одной транзакции с максимальной суммой**. Это не категория с максимальной суммой всех операций.
- Основной запуск hw2: Kafka + ClickHouse + Python tools. ML, PostgreSQL и UI не требуются. При изменении Compose сохранить прежний запуск в `docker-compose.hw1.yml` и оставить старую реализацию.

## 2. Среда и установка ClickHouse

Проверено 12.10.2026:

| Проверка | Результат |
| --- | --- |
| ОС / оболочка | Windows / PowerShell |
| Docker CLI | Есть, `27.5.1`, контекст `desktop-linux` |
| Docker Compose | Есть, `v2.32.4-desktop.1` |
| Docker Engine | Доступен после запуска Docker Desktop; Linux, 4 CPU, около 6 ГБ RAM |
| WSL | В списке есть `docker-desktop` |
| `clickhouse` / `clickhouse-client` | Не найдены в PATH |
| Образы / контейнеры ClickHouse | Образ `26.8.20.9` скачан, контейнер healthy, Kafka Engine проверен |

Отсутствие команды в PATH не доказывает отсутствие установки во всех каталогах или Docker-образа. Развернуть ClickHouse **контейнером в проектном Compose**: официальный `clickhouse/clickhouse-server` содержит клиент. Отдельная установка на Windows для задания не потребуется. См. [ClickHouse в Docker](https://clickhouse.com/docs/get-started/setup/self-managed/docker).

1. Запустить установленный Docker Desktop, дождаться Linux Engine. При ошибке проверить WSL 2 по [инструкции Docker для Windows](https://docs.docker.com/desktop/setup/install/windows-install/).
2. Проверить `docker version`, `docker info`, `docker compose version`: нужен ответ **Server**, одного Client недостаточно.
3. Проверить `docker ps -a`, `docker image ls`: старые Kafka/ClickHouse, порты, конфликтующий Compose-проект. Не удалять чужие ресурсы.
4. Проверить порты Kafka `9094`, ClickHouse HTTP `8123`; публиковать только на `127.0.0.1`. Native-порт `9000` наружу для команд через контейнер не нужен.
5. Закрепить полный стабильный тег ClickHouse после проверки доступности, архитектуры и запуска. Не угадывать тег и не оставлять `latest`. Проверить существующий `apache/kafka:4.3.1`, Python и зависимости при сборке.
6. Начать примерно с 4 CPU и 6–8 ГБ памяти, уточнить по измерениям. ClickHouse/Kafka хранят данные в именованных томах; CSV/результаты монтируются отдельно.

Этап установки окружения завершён: образы скачаны, контейнеры запущены и проверены. Шаги выше оставлены как инструкция для повторного запуска.

## 3. Данные и контракт

До окончательного DDL получить CSV через Kaggle с пользовательским доступом и положить в игнорируемую папку `data/`. Если уже скачан — использовать локальный файл. Токены/исходный архив в Git не включать.

Потоковый профиль данных определяет:

- реальные имена колонок, разделитель, кодировку/BOM, индексную колонку;
- число строк, обязательные пропуски, набор штатов/категорий;
- диапазон/точность сумм, отрицательные/неконечные значения;
- форматы/диапазон времени, timezone;
- исходный ID, дубликаты ID/строк;
- какие CSV нужны. Не объединять train/test без описания выбранного набора.

Сохранить `reports/dataset_profile.json`: источник, SHA-256, выбранный диапазон, число валидных строк и статистики без персональных данных. Имена сопоставлять явно, неизвестная схема — ошибка.

| Поле | Правило |
| --- | --- |
| `dataset_id` | Хеш файла и выбранного диапазона: демо, полный набор и префикс различаются |
| `transaction_id` | Стабильный хеш dataset и номера записи CSV; исходный ID при наличии хранить отдельно |
| `us_state` | Непустой код, trim/uppercase; допустимые значения установить после профиля, учесть DC/территории |
| `cat_id` | Непустая категория без произвольного переименования |
| `amount` | `Decimal64(2)` при подтверждённых двух знаках и диапазоне; иначе пересмотреть precision/scale |
| `transaction_time` | `DateTime` или `DateTime64` после проверки точности; явное правило timezone |
| Остальные колонки | Сохранить исходные поля с типами после проверки файла |
| Метаданные Kafka | Топик, partition, offset для диагностики |

Dataset изолирует результат от демо/частичных загрузок. ID не зависит от времени запуска; диапазон входит в dataset, чтобы первые 100 строк не смешивались с полным файлом.

Сумму читать Python `Decimal` из строки, отправлять десятичной строкой в JSON. Не использовать промежуточный `float`, не округлять лишние знаки молча. Проверить [ограничения Decimal](https://clickhouse.com/docs/reference/data-types/decimal). Отрицательные конечные суммы поддержать при наличии: обычный максимум, не максимум модуля.

Для времени без timezone принять одно техническое правило в producer/БД/эталоне; не придумывать преобразования по штату.

## 4. Архитектура и инициализация

```text
CSV → load_csv_to_kafka.py → Kafka: transactions_analytics
                                      ↓
                    transactions_kafka (ENGINE = Kafka)
                                      ↓ materialized view
                    transactions (MergeTree)
                                      ↓ MV + argMaxState
                    state_max (AggregatingMergeTree)
                                      ↓ argMaxMerge
                    SQL → max_transaction_by_state.csv
```

Новый топик принимает плоский `JSONEachRow`. Старый `transactions` содержит вложенный `{"transaction_id": ..., "data": ...}` для модели; смешивать контракты нельзя.

Сервисы основного Compose:

- `kafka-volume-init`, `kafka`, `kafka-init`: переиспользовать KRaft, создать аналитический топик;
- `clickhouse`: официальный образ, том, healthcheck с `SELECT 1`, пользователь/пароль из `.env`;
- `clickhouse-init`: одноразовый final SQL после готовности ClickHouse и успешного Kafka init;
- `tools`: собственный Python-образ с корневым requirements, скриптами/тестами; по требованию через `docker compose run --rm tools ...`;
- профиль `benchmark` для контрольной таблицы, если нужен; основной запуск не хранит лишнюю копию.

Внутри сети: `kafka:19092`, `clickhouse:8123`; с Windows broker — `localhost:9094`. Healthcheck использует рабочую аутентификацию. Готовность — условия `depends_on`, не порядок строк YAML. Подготовку `.env` описать для PowerShell/bash.

Порядок DDL: база → таблица транзакций → агрегаты → MV транзакций в агрегаты → Kafka-таблица → MV Kafka в транзакции. Последний шаг включает потребление, когда назначения готовы. Исполнять через `clickhouse-client --multiquery`; ошибка SQL прерывает init.

Kafka Engine: `JSONEachRow`, отдельная постоянная consumer group, сначала один consumer/partition. Он читает поток, MergeTree через MV хранит записи. Отчёты читать из сохранённых таблиц. См. [Kafka Engine](https://clickhouse.com/docs/reference/engines/table-engines/integrations/kafka).

## 5. Отдельный producer

`scripts/load_csv_to_kafka.py`: путь CSV, broker/topic, необязательный лимит, таймауты. Документированные defaults; без лимита — весь выбранный файл.

1. Первый потоковый проход: схема/выбранные записи, хеш, ожидаемое число сообщений. Ошибка с номером строки/полем завершает работу до публикации.
2. Второй проход: JSON на запись, dataset/стабильный ID/типизированные значения. CSV целиком в память не загружать, проверить неизменность файла между проходами.
3. `acks=all`, producer idempotence, delivery callback, `poll()`, backpressure при заполнении очереди, проверка `flush()`.
4. Failure доставки — ненулевой exit code. Число вызовов `produce()` не равно числу подтверждений.
5. Манифест: источник/хеш/dataset/диапазон/expected/confirmed. ID пересчитываются потоково вместо большого списка.

Валидация предотвращает частичную отправку неверного CSV; сетевой сбой во втором проходе всё равно может оставить часть сообщений. Это отражать в манифесте/логах.

Kafka Engine допускает повторы. MergeTree не обеспечивает уникальность ID; producer idempotence не устраняет повторные запуски. Raw может содержать дубликаты. Максимум **неизменяемых** сообщений устойчив к одинаковым повторам; суммы/счётчики — нет. Не обещать exactly-once и не считать Kafka key дедупликацией.

Сначала строгая ошибка на плохом сообщении, без молчаливого `kafka_skip_broken_messages`. Неверный JSON проверять в отдельном topic/group, диагностировать ошибку; очищать только тестовые ресурсы.

## 6. SQL и одинаковые суммы

Результат: `us_state, cat_id, max_amount, transaction_id`, строка на штат выбранного dataset, `ORDER BY us_state`. ID показывает, что сумма/категория относятся к одной операции.

При равенстве сумм выбираем **лексикографически наибольший transaction_id**. Единое техническое правило Python/SQL. Если потребуются все категории равного максимума — пересмотреть контракт/тесты до финальной схемы.

Основа прямого запроса:

```sql
SELECT
    us_state,
    argMax(
        tuple(toString(cat_id), amount, transaction_id),
        tuple(amount, transaction_id)
    ) AS winner
FROM fraud_analytics.transactions
WHERE dataset_id = {dataset_id:String}
GROUP BY us_state
ORDER BY us_state;
```

Final SQL развернёт winner в именованные поля, экспорт — `CSVWithNames`. Простой `argMax(cat_id, amount)` не задаёт устойчивый выбор равных максимумов. См. [argMax](https://clickhouse.com/docs/reference/functions/aggregate-functions/argMax).

Витрина: `argMaxMerge(winner_state)`, `GROUP BY us_state`, фильтр dataset. Аргумент состояния — `Tuple(String, Decimal64(2), String)`, ключ сравнения — `Tuple(Decimal64(2), String)`, если профиль подтвердил деньги. В MV категория явно `String`, чтобы LowCardinality не создал несовместимый тип. Проверить `toTypeName` и настоящий DDL.

## 7. Оптимизация и её проверка

### 7.1. Основная таблица

Baseline `001_baseline.sql`: корректные типы денег/времени, обычные String для штата/категории, `ORDER BY (dataset_id, transaction_time, transaction_id)`, стандартное сжатие. Те же логические поля/строки; точность денег не ухудшать ради сравнения.

Final `002_optimized.sql`:

| Изменение | Ожидаемая польза | Ограничение/проверка |
| --- | --- | --- |
| LowCardinality(String) штат/категория | Словарное кодирование повторов | Кардинальности/байты; не применять автоматически к адресам/ID |
| ORDER BY (dataset_id, us_state, transaction_time, transaction_id) | Локальность штата, выбранного dataset, сжатие | План/чтение; полный максимум всё ещё может читать весь dataset |
| ZSTD(1) подходящих строк | Меньше места | Сравнить с базовым кодеком, учитывать CPU |
| Delta, ZSTD(1) времени | Сжатие близких меток | Применимость типа и реальный выигрыш |
| Узкие типы других чисел | Меньше места при том же диапазоне | По профилю; точность координат произвольно не урезать |

Сначала **без PARTITION BY**. Месячные партиции — при обоснованном удалении/фильтрации по времени; целевой запрос месяцем не ограничен. Много партиций/мелких parts могут ухудшить работу. Витрина также без временного partition key.

Основания: [LowCardinality](https://clickhouse.com/docs/reference/data-types/lowcardinality), [сжатие](https://clickhouse.com/docs/guides/clickhouse/data-modelling/compression/compression-in-clickhouse), [партиции](https://clickhouse.com/docs/concepts/best-practices/partitioning-keys). Ключи/кодеки — наши гипотезы для измерения.

### 7.2. Витрина целевого запроса

`state_max`: AggregatingMergeTree, `ORDER BY (dataset_id, us_state)`, состояние argMax на пару dataset/штат. MV из transactions группирует каждый вставленный блок и пишет `argMaxState`.

Запрос читает частичные максимумы вместо всех операций. До merges строк на штат может быть несколько: `argMaxMerge` нужен независимо от их завершения. FINAL/OPTIMIZE FINAL не требуются для штатной корректности. См. [AggregatingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/aggregatingmergetree).

Цена — дополнительная запись/место и специализация под запрос. Полная таблица остаётся. Старые данные сами не попадут в новую incremental MV; удаления/изменения источника автоматически максимум не пересчитают. Контракт — append-only неизменяемые записи. Для backfill остановить приём и контролируемо заполнить витрину; при изменениях источника перестроить. См. [incremental MV](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view).

ReplacingMergeTree не обещает мгновенную уникальность: удаление дублей при merge не отменяет уже обработанные downstream MV вставки. Здесь одинаковые повторы безопасны благодаря максимуму.

### 7.3. Измерения

`benchmark_clickhouse.py` сравнивает на одном dataset:

1. Baseline + прямой argMax.
2. Оптимизированную таблицу + тот же прямой argMax.
3. Витрину + argMaxMerge.

Сначала точное совпадение результатов. Baseline наполнить копией загруженных строк или теми же фиксированными сообщениями в отдельной consumer group, проверить равенство данных. Два потребителя одной группы не получают каждый весь поток.

- Дождаться загрузки, зафиксировать dataset/число строк/version/CPU/RAM/settings/parts.
- Не измерять при загрузке, отключить query result cache.
- Прогрев, минимум 7 чередующихся запусков каждого пути, уникальные query_id, одинаковые threads/memory.
- Медиана/диапазон времени, read_rows/read_bytes/memory_usage из завершённых `system.query_log`; SYSTEM FLUSH LOGS, исключить служебные запросы.
- Active parts и compressed/uncompressed bytes через system.parts/system.parts_columns.
- Размер baseline, оптимизированной таблицы, витрины и **общий размер transactions + state_max**. Витрина не заменяет полное хранение.
- `reports/benchmark.csv`, методика в README. Не обещать заранее «10×»: демо может быть ниже точности таймера.
- Принудительные merges для сжатия — отдельный отмеченный эксперимент, одинаковый для таблиц. Корректность сначала без них.

Поля: [system.query_log](https://clickhouse.com/docs/reference/system-tables/query_log). Объём чтения отделяет уменьшение работы от случайного выигрыша времени. Демо — корректность, реальный CSV — эффект; синтетические измерения подписывать отдельно.

## 8. Тесты DDL, SQL и всей цепочки

DDL — исполняемый код: SQL запускается на **том же закреплённом ClickHouse**, что и проект. Regex-проверки текста недостаточны.

| Проверка | Сценарий/ожидание |
| --- | --- |
| Compose/сборка | docker compose config --quiet, tools build, чистый старт; healthy/init exit 0 |
| DDL на пустой базе | Baseline/final SQL; system.tables, system.columns, SHOW CREATE TABLE: engines/types/sorting/codecs/MV/назначения |
| Повтор DDL | Без ошибок/потери записей/лишних объектов; IF NOT EXISTS не миграция, проверять несовпадение metadata |
| Ошибка DDL | Неверный SQL → ненулевой exit code, проект не объявлен готовым |
| Producer | BOM, кавычки/запятые, перестановка допустимых колонок, пустой CSV, пропуски, неверные суммы/даты, ID, failure доставки |
| Decimal | 0.10/0.20, отрицательные суммы, границы диапазона/overflow; без потери точности |
| Максимумы | Несколько штатов/категорий, равные суммы, штат с одной записью; Python-эталон |
| До merges | Несколько блоков; argMaxMerge правилен сразу и после merges |
| Новый максимум | Меньшая/большая сумма, новый штат, равная сумма/другой ID |
| Kafka → ClickHouse | CSV через отдельный producer; ожидаемые уникальные ID, значения и итог |
| Replay | Повтор CSV; те же ID, raw может дублироваться, максимум неизменен |
| Рестарты | Kafka/ClickHouse: данные сохранены, новые сообщения обрабатываются |
| Плохой JSON | Изолированный topic/group; диагностируемая ошибка и failure |
| Dataset | Демо/префикс/полный набор не смешиваются |
| Экспорт | Отдельный SQL, CSV прочитать обратно: заголовок/порядок/Decimal/эталон |

Организация:

- `tests/test_csv_loader.py`: meaningful unit-проверки Decimal/ID/контракта/delivery failures.
- `tests/integration/test_ddl.py`: реальный сервер, уникальная база; контролируемая подстановка database/topic/group. Не делить SQL через ненадёжный `split(';')`.
- `tests/integration/test_pipeline.py`: отдельные topic/group/dataset, публичные producer/DDL/SQL.
- `examples/analytics_demo.csv`, `tests/fixtures/`: синтетические записи, включая равенства; без персональных строк Kaggle.
- Независимый эталон читает CSV через csv/Decimal и выбирает максимум (amount, transaction_id). Не вызывает ClickHouse и не ограничивается двумя SQL.
- Deadline, например 120 секунд, polling условия готовности. Timeout: expected/actual unique count, dataset, init logs, system.kafka_consumers, состояние группы. sleep(10) не заменяет проверку.
- На чистом запуске проверить raw count; при retry/replay показывать raw/unique отдельно. Проверять набор ID и значения, не только число.
- Пустая база/отсутствующий dataset не выдаёт старый итог. Пустой вход producer отклоняет понятно.
- Отдельный Compose project name/тома, cleanup только своих ресурсов. Рабочий down сохраняет данные; down -v обозначить удалением тестовых данных.
- Старые тесты остаются hw1. Команда hw2 выбирает новые тесты и не требует CatBoost ради DDL.

## 9. Экспорт и файлы результата

`export_results.py` принимает успешный манифест, ждёт полноты dataset по уникальным ID с deadline, исполняет отдельный `004_max_transaction_by_state.sql` с параметром dataset. Не дублировать SQL в Python.

Пишет UTF-8 CSVWithNames в `results/max_transaction_by_state.csv`; metadata источника/числа строк/времени/версии SQL — отдельно. Сверяет с независимым расчётом CSV, расхождение — failure.

Для сдачи нужен **реальный набор задания**. Демо-итог хранить отдельно и подписать; пока реального файла нет, этот критерий незавершён.

В gitignore: исходные CSV/ZIP, .env, тома/data; точечные исключения для fixtures, итогового CSV, reports/benchmark.csv. Проверить через git check-ignore, что результаты отслеживаются.

## 10. Структура итогового проекта

```text
docker-compose.yml                    # hw2
docker-compose.hw1.yml                # прежний стек
Dockerfile                            # tools
requirements.txt                      # закреплённые runtime версии
requirements-dev.txt                  # тесты, при необходимости
.env.example
README.md
PLAN.md
TASK.md                               # hw2, прежнее описание в docs/
scripts/
  profile_dataset.py
  load_csv_to_kafka.py
  export_results.py
  benchmark_clickhouse.py
sql/clickhouse/
  001_baseline.sql
  002_optimized.sql                   # самодостаточный final DDL + ingestion
  003_max_transaction_by_state_raw.sql
  004_max_transaction_by_state.sql    # запрос витрины
tests/
  test_csv_loader.py
  integration/test_ddl.py
  integration/test_pipeline.py
  fixtures/
examples/analytics_demo.csv
results/max_transaction_by_state.csv
reports/dataset_profile.json
reports/load_manifest.json
reports/benchmark.csv
data/                                # локальный CSV, игнорируется Git
```

Основной init запускает самодостаточный 002_optimized.sql. Baseline — сравнение, не обязательный шаг пользователя. Config/init-скрипт добавить при необходимости аутентификации/параметров.

Зависимости: confluent-kafka уже используется; выбрать ClickHouse/HTTP-клиент с параметрами и корректным Decimal, тестовый runner. Версии закрепить после проверки. Большой pandas/ML-стек загрузчику не нужен.

## 11. Порядок реализации

1. **Среда/данные.** Engine, CSV/образы, профиль/контракт. Готовность: доступны Engine/CSV, подтверждены поля/деньги/время.
2. **Минимальная цепочка.** Producer, topic, ClickHouse, MergeTree/MV. Демо через Kafka сохранено, init воспроизводим.
3. **Запрос/эталон.** Прямой SQL, Python-расчёт/экспорт. Совпадение результатов, включая равенства.
4. **Оптимизация.** Типы/ключи/кодеки, агрегированная MV. Идентичный итог до/после без ожидания merges.
5. **Проверки.** DDL/re-init/Kafka/replay/рестарты/ошибки/dataset на закреплённых образах.
6. **Реальный набор/измерения.** Полный выбранный CSV, итог/эталон, три пути/размеры. Сохранены результаты с происхождением.
7. **Документация/сдача.** TASK/README, чистый запуск, коммит и публикация ветки/PR. Проверка без старого ML-окружения.

README: проверенные PowerShell/bash команды для .env, старта/ожидания init, демо/реального CSV из data, экспорта, тестов, benchmark, логов, остановки и сброса тестовых ресурсов. CSV пишет Python: кодировка не зависит от shell-redirection.

## 12. Критерии готовности

- [x] Engine доступен, ClickHouse запускается проверенным закреплённым образом.
- [x] Реальная схема/источник/хеш/файлы/диапазон подтверждены (для hw2 выбран полный test.csv).
- [x] Отдельный producer потоково загружает CSV, проверяет подтверждения.
- [x] Final DDL создаёт Kafka/MergeTree/агрегаты/MV в правильном порядке.
- [x] Основной SQL возвращает категорию максимальной операции по штату с правилом равенства.
- [x] Реальный итоговый CSV в Git, совпадает с независимым эталоном.
- [x] Оптимизация в DDL, польза/цена измерены.
- [x] Реальные DDL/integration/replay/restart проверки проходят.
- [x] README/requirements/Compose позволяют запуск с нуля.
- [x] Нет исходного датасета/секретов/томов в Git, hw1 доступно отдельно.
- [x] Проверена чистая копия с новыми томами; ветка готова к публикации, ссылка сдачи указана в README.

## 13. Решения по ключевым вопросам

| Вопрос | Решение |
| --- | --- |
| Что скачивается по Kaggle? | Проверить с доступом, схему не угадывать |
| Равные суммы? | Наибольший стабильный ID, единое правило |
| Как тестируем DDL? | Реальный сервер, metadata и вставки/поведение MV |
| Чем ускоряем главный запрос? | argMaxState при записи, argMaxMerge при чтении |
| Партиции/skipping indexes? | По умолчанию нет: полный максимум без полезного фильтра |
| Повторы? | Raw может дублироваться, максимум неизменяемых записей устойчив |
| Достаточно демо? | Для запуска да, для сдачи реальный CSV |
| ClickHouse на Windows? | Проектный контейнер со встроенным клиентом |
| Демо/частичная/полная загрузка? | Разные dataset ID, фильтр экспорта |
| Что пока неизвестно? | Эффект оптимизации и поведение постоянной схемы: проверить на следующих этапах |

## Реализованные уточнения

- Общий CSV reader извлечён из interface/app.py; новый CLI использует его и сохраняет все 17 полей и optional target.
- Загрузка делает два CSV-прохода плюс проверку хеша после публикации для обнаружения изменения файла.
- Compose запускается через up -d: в проверенной версии --wait выдаёт ошибку для завершившегося init даже с кодом 0.
- Объём raw/aggregate в storage.csv включает 9 demo-записей; baseline содержит только test. В benchmark query_log зафиксировано 56 прочитанных состояний при результате 51.
- Независимая проверка сопоставляет все уникальные строки и каждый столбец, а затем raw SQL, aggregate SQL и экспорт CSV.

- Проверка чистой копии a2f344c: новые тома hw2-acceptance, build, environment check, demo producer/export и 4 интеграционных теста прошли. Отчёт: reports/validation.json; временные контейнеры/тома удалены.
- .gitattributes сохраняет точные байты CSV: SHA-256/ID не зависят от автоконвертации переводов строк Git.
