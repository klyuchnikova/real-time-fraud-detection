CREATE DATABASE IF NOT EXISTS {database} ENGINE = Atomic;
CREATE TABLE IF NOT EXISTS {database}.transactions (
    transaction_id UInt64,
    client_id UInt64,
    transaction_time DateTime('UTC'),
    merch String,
    cat_id LowCardinality(String),
    amount Decimal(18, 2),
    name_1 String,
    name_2 String,
    gender LowCardinality(String),
    street String,
    one_city String,
    us_state LowCardinality(String),
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
) ENGINE = MergeTree
ORDER BY (us_state, transaction_time, transaction_id);
CREATE TABLE IF NOT EXISTS {database}.clients (
    client_id UInt64,
    name_1 String,
    name_2 String,
    street String,
    one_city String,
    us_state String,
    post_code String
) ENGINE = MergeTree ORDER BY client_id;
CREATE TABLE IF NOT EXISTS {database}.ingestion_meta (
    source_sha256 String,
    registry_uuid String,
    source_rows UInt64,
    fraud_rows UInt64,
    total_amount Decimal(24, 2),
    client_count UInt64
) ENGINE = MergeTree ORDER BY source_sha256;
