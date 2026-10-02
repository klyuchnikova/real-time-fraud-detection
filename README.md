# Потоковый скоринг транзакций

Учебный сервис читает транзакции из Kafka, считает вероятность фрода готовой моделью CatBoost и сохраняет результат в Kafka и PostgreSQL. В Streamlit можно отправить CSV и посмотреть последние результаты.

```text
CSV → Streamlit → Kafka: transactions → scoring-service → Kafka: scores
                                                          ↓
                                                    scores-sink
                                                          ↓
                                                  PostgreSQL → Streamlit
```

Модель работает на CPU. Обучения внутри сервиса нет. Код подготовки признаков основан на [проекте семинара](https://github.com/NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection), коммит `41540d91f0cbde0106671d3eb8270403508fade4`; из него также взята модель `my_catboost.cbm`. Статистики из обучающего набора заранее сохранены в `artifacts/preprocessor.json`, поэтому для обычного запуска `train.csv` не нужен.

## Быстрый запуск

Нужен Docker Desktop с Docker Compose. Для Mac с Apple Silicon используйте [официальную установку Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/). После первого запуска приложения проверьте в терминале:

```bash
docker version
docker compose version
docker info
```

Из корня этого репозитория выполните:

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps -a
```

При первом запуске загрузка образов и сборка могут занять несколько минут. Дождитесь, пока `kafka` и `postgres` станут `healthy`, одноразовые `kafka-volume-init` и `kafka-init` завершатся с кодом 0, а остальные сервисы будут `running`. Откройте [интерфейс](http://localhost:8501).

Во вкладке **«Отправить транзакции»** загрузите [examples/demo.csv](examples/demo.csv) и нажмите **«Отправить в Kafka»**. Затем во вкладке **«Результаты»** нажмите **«Посмотреть результаты»**. Демо содержит три вымышленные транзакции. Список фродовых транзакций может быть пустым: он показывает только результаты, которые модель пометила флагом 1. Гистограмма покажет полученные скоры.

Автоматическая проверка всей цепочки также доступна одной командой:

```bash
docker compose exec -T interface python -m scripts.smoke_stack
```

Она отправляет одну вымышленную транзакцию и ждёт запись в PostgreSQL. После перезапуска потребителей присоединение к Kafka может занять около минуты; проверка ждёт до 120 секунд.

Для проверки на данных соревнования распакуйте локальный архив и загрузите `test.csv` в интерфейсе:

```bash
unzip -p teta-ml-1-2025.zip test.csv > test.csv
```

Если архива нет, получите его на [странице соревнования](https://www.kaggle.com/competitions/teta-ml-1-2025). Файл содержит 262 144 строки; для быстрой проверки начните со 100 строк. Для проверки непустого списка фрода можно отправить 1000 строк: на первых 1000 строках данного архива модель поставила 5 флагов. Это проверка работы сервиса, а не оценка качества модели. Исходные CSV и ZIP исключены из Git.

## Как проверить результат

Сообщения в топике `scores` содержат по одному JSON-объекту с полями `transaction_id`, `score`, `fraud_flag`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic scores --from-beginning --max-messages 1 --timeout-ms 10000
```

Проверьте витрину в PostgreSQL:

```bash
docker compose exec postgres psql -U fraud -d fraud -c 'SELECT transaction_id, score, fraud_flag FROM transaction_scores ORDER BY received_order DESC LIMIT 10;'
```

Если результаты не появились сразу, подождите несколько секунд и обновите их кнопкой в интерфейсе. Для диагностики:

```bash
docker compose logs --tail=100 scoring-service scores-sink
docker compose ps
```

Остановка без удаления данных:

```bash
docker compose down
```

## Формат и обработка

`transactions` принимает один JSON-объект на транзакцию: `{"transaction_id":"<id>","data":{...}}`. Поле `data` содержит 17 колонок из `test.csv`. Интерфейс сам создаёт ID. Скоринг использует подготовку времени, категорий, географического расстояния и числовых значений из семинара. Порог для `fraud_flag` — `0.98`; его можно изменить через `FRAUD_THRESHOLD` в `docker-compose.yml`.

`scores` содержит ровно три поля: `transaction_id`, `score` от 0 до 1 и `fraud_flag` со значением 0 или 1. Отдельный `scores-sink` записывает их в PostgreSQL. При повторе сообщения строка с тем же `transaction_id` не дублируется. Витрина хранит время и порядок получения; интерфейс читает из неё 10 последних записей с флагом 1 и скоры последних 100 записей. Некорректное сообщение пропускается с предупреждением в логах и подтверждением offset; сервисы не пытаются обучать модель на потоке.

Kafka доступна с компьютера на `localhost:9094`, Streamlit — на `localhost:8501`. PostgreSQL доступна только контейнерам сети Compose. `.env` содержит локальный пароль БД и не отслеживается Git.

## Структура проекта

- `scoring_service/`: чтение Kafka, подготовка признаков, модель и публикация скора.
- `scores_sink/`: чтение `scores` и запись в PostgreSQL.
- `interface/`: отправка CSV и просмотр результатов.
- `artifacts/`: готовая модель и сохранённые статистики предобработки.
- `sql/`: создание витрины.
- `scripts/export_preprocessor.py`: одноразовое обновление статистик из `train.csv`.
- `examples/demo.csv`: небольшой вымышленный пример для проверки запуска.

Для повторного создания `artifacts/preprocessor.json` нужен архив в корне проекта и Python 3.12. Это действие выполняется отдельно от работы сервиса:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r scoring_service/requirements.txt
.venv/bin/python -m scripts.export_preprocessor
```

После создания артефакта можно запустить локальные проверки:

```bash
.venv/bin/python -m pip install -r scores_sink/requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

Подробные шаги разработки и критерии приёмки находятся в [PLAN.md](PLAN.md).
