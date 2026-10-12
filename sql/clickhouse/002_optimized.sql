-- Standalone final DDL. All destinations exist before Kafka consumption starts.
CREATE DATABASE IF NOT EXISTS fraud_analytics;

CREATE TABLE IF NOT EXISTS fraud_analytics.transactions
(
    dataset_id String CODEC(ZSTD(1)),
    row_index UInt64 CODEC(Delta, ZSTD(1)),
    transaction_id String CODEC(ZSTD(1)),
    transaction_time DateTime('UTC') CODEC(Delta, ZSTD(1)),
    merch String CODEC(ZSTD(1)),
    cat_id LowCardinality(String) CODEC(ZSTD(1)),
    amount Decimal64(2) CODEC(ZSTD(1)),
    name_1 String CODEC(ZSTD(1)),
    name_2 String CODEC(ZSTD(1)),
    gender LowCardinality(String) CODEC(ZSTD(1)),
    street String CODEC(ZSTD(1)),
    one_city String CODEC(ZSTD(1)),
    us_state LowCardinality(String) CODEC(ZSTD(1)),
    post_code String CODEC(ZSTD(1)),
    lat Float64 CODEC(ZSTD(1)),
    lon Float64 CODEC(ZSTD(1)),
    population_city UInt32 CODEC(ZSTD(1)),
    jobs String CODEC(ZSTD(1)),
    merchant_lat Float64 CODEC(ZSTD(1)),
    merchant_lon Float64 CODEC(ZSTD(1)),
    target Nullable(UInt8) CODEC(ZSTD(1)),
    kafka_topic LowCardinality(String) CODEC(ZSTD(1)),
    kafka_partition UInt32 CODEC(ZSTD(1)),
    kafka_offset UInt64 CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (dataset_id, us_state, transaction_time, transaction_id);

CREATE TABLE IF NOT EXISTS fraud_analytics.state_max
(
    dataset_id String CODEC(ZSTD(1)),
    us_state LowCardinality(String),
    winner_state AggregateFunction(argMax,
        Tuple(String, Decimal64(2), String),
        Tuple(Decimal64(2), String))
)
ENGINE = AggregatingMergeTree
ORDER BY (dataset_id, us_state);

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_analytics.transactions_to_state_max
TO fraud_analytics.state_max
AS SELECT
    dataset_id, us_state,
    argMaxState(tuple(toString(cat_id), amount, transaction_id),
                tuple(amount, transaction_id)) AS winner_state
FROM fraud_analytics.transactions
GROUP BY dataset_id, us_state;

CREATE TABLE IF NOT EXISTS fraud_analytics.transactions_kafka
(
    dataset_id String,
    row_index UInt64,
    transaction_id String,
    transaction_time DateTime('UTC'),
    merch String,
    cat_id String,
    amount Decimal64(2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target Nullable(UInt8)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'transactions_analytics',
    kafka_group_name = 'fraud_analytics_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_poll_max_batch_size = 65536,
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 0,
    kafka_handle_error_mode = 'default';

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_analytics.kafka_to_transactions
TO fraud_analytics.transactions
AS SELECT
    dataset_id, row_index, transaction_id, transaction_time, merch, cat_id, amount, name_1, name_2, gender, street, one_city, us_state, post_code, lat, lon, population_city, jobs, merchant_lat, merchant_lon, target,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    _offset AS kafka_offset
FROM fraud_analytics.transactions_kafka;
