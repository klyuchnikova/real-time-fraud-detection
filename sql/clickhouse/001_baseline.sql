-- Comparison table only. Normal startup uses 002_optimized.sql.
CREATE DATABASE IF NOT EXISTS fraud_analytics;

CREATE TABLE IF NOT EXISTS fraud_analytics.transactions_baseline
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
    target Nullable(UInt8),
    kafka_topic String,
    kafka_partition UInt32,
    kafka_offset UInt64
)
ENGINE = MergeTree
ORDER BY (dataset_id, transaction_time, transaction_id);
