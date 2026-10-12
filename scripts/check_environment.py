"""Verify ClickHouse DDL/codecs/Decimal and Kafka delivery in temporary resources."""
import json
import os
import time
import uuid
from decimal import Decimal
from importlib.metadata import version

import clickhouse_connect
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def main():
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    client = clickhouse_connect.get_client(
        host=host, port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "analytics"),
        password=os.environ["CLICKHOUSE_PASSWORD"], database="fraud_analytics",
    )
    suffix = uuid.uuid4().hex
    table = f"environment_probe_{suffix}"
    topic = f"environment-probe-{suffix}"
    queue = f"environment_queue_{suffix}"
    view = f"environment_view_{suffix}"
    consumer = None
    created_topic = False
    created_table = False
    created_queue = False
    created_view = False
    admin = AdminClient({"bootstrap.servers": broker})
    try:
        ch_version = client.query("SELECT version()").first_row[0]
        engines = {row[0] for row in client.query(
            "SELECT name FROM system.table_engines WHERE name IN ('Kafka', 'MergeTree', 'AggregatingMergeTree')"
        ).result_rows}
        if engines != {"Kafka", "MergeTree", "AggregatingMergeTree"}:
            raise RuntimeError(f"Missing ClickHouse engines: {engines}")
        client.command(
            f"CREATE TABLE {table} (state LowCardinality(String), "
            "amount Decimal64(2), ts DateTime('UTC') CODEC(Delta, ZSTD(1))) "
            "ENGINE = MergeTree ORDER BY (state, ts)"
        )
        created_table = True
        client.insert(table, [["DC", Decimal("0.10"), 1546300800],
                              ["TX", Decimal("123.45"), 1546300860]],
                      column_names=["state", "amount", "ts"])
        rows = client.query(f"SELECT state, amount FROM {table} ORDER BY state").result_rows
        if rows != [("DC", Decimal("0.10")), ("TX", Decimal("123.45"))]:
            raise AssertionError(f"Decimal round trip failed: {rows}")
        futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        futures[topic].result(30)
        created_topic = True
        client.command(
            f"CREATE TABLE {queue} (state String, amount Decimal64(2), ts DateTime('UTC')) "
            "ENGINE = Kafka SETTINGS "
            f"kafka_broker_list = '{broker}', kafka_topic_list = '{topic}', "
            f"kafka_group_name = 'clickhouse-environment-{suffix}', kafka_format = 'JSONEachRow', "
            "kafka_num_consumers = 1, kafka_flush_interval_ms = 500"
        )
        created_queue = True
        client.command(f"CREATE MATERIALIZED VIEW {view} TO {table} AS SELECT state, amount, ts FROM {queue}")
        created_view = True
        errors = []
        producer = Producer({"bootstrap.servers": broker, "enable.idempotence": True, "acks": "all"})
        payload = json.dumps({"state": "CA", "amount": "0.20", "ts": "2019-01-01 00:02:00"}).encode()
        producer.produce(topic, key=suffix, value=payload,
                         on_delivery=lambda err, msg: errors.append(str(err)) if err else None)
        if producer.flush(30) or errors:
            raise RuntimeError(f"Kafka delivery failed: {errors}")
        consumer = Consumer({
            "bootstrap.servers": broker, "group.id": f"environment-probe-{suffix}",
            "auto.offset.reset": "earliest", "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        })
        consumer.subscribe([topic])
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            if message.value() != payload:
                raise AssertionError("Kafka payload differs")
            break
        else:
            raise TimeoutError("Kafka message not received within 60 seconds")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            count = client.query(
                f"SELECT count() FROM {table} WHERE state = 'CA' AND amount = toDecimal64('0.20', 2)"
            ).first_row[0]
            if count:
                break
            time.sleep(0.5)
        else:
            raise TimeoutError("Kafka Engine did not ingest the test message within 60 seconds")
        report = {
            "status": "ok", "clickhouse_version": ch_version,
            "clickhouse_engines": sorted(engines), "decimal_round_trip": True,
            "kafka_round_trip": True, "kafka_engine_ingestion": True,
            "packages": {name: version(name) for name in ("clickhouse-connect", "confluent-kafka")},
        }
    finally:
        if consumer is not None:
            consumer.close()
        if created_view:
            client.command(f"DROP VIEW {view} SYNC")
        if created_queue:
            client.command(f"DROP TABLE {queue} SYNC")
        if created_topic:
            admin.delete_topics([topic])[topic].result(30)
        if created_table:
            client.command(f"DROP TABLE {table} SYNC")
        client.close()
    report["temporary_resources_cleaned_on_exit"] = True
    output = os.getenv("ENVIRONMENT_REPORT", "reports/environment_check.json")
    from pathlib import Path
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    Path(output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
