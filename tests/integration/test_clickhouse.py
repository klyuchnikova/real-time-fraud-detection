"""Execute the actual DDL against isolated Kafka topics and ClickHouse databases."""
import json
import os
import tempfile
import time
import unittest
import uuid
from decimal import Decimal
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common.analytics import ALL_COLUMNS, iter_csv, parse_row, transaction_id
from scripts.clickhouse_support import SQL_DIR, execute_ddl, get_client, query_file, render_ddl, wait_dataset
from scripts.export_results import export_results
from scripts.load_csv_to_kafka import load_file


class ClickHouseTests(unittest.TestCase):
    def setUp(self):
        suffix = uuid.uuid4().hex
        self.db, self.topic = "test_hw2_" + suffix, "test_hw2_" + suffix
        self.broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
        self.admin = AdminClient({"bootstrap.servers": self.broker})
        self.client = get_client()
        self.addCleanup(self.client.close)
        self.addCleanup(self.cleanup_resources)
        self.admin.create_topics([NewTopic(self.topic, 1, 1)])[self.topic].result(30)
        self.ddl = render_ddl(SQL_DIR / "002_optimized.sql", self.db, self.topic,
                              "group_" + suffix, self.broker)
        execute_ddl(self.client, self.ddl)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.manifest = Path(self.temp.name) / "manifest.json"

    def cleanup_resources(self):
        self.client.command(f"DROP DATABASE IF EXISTS {self.db} SYNC")
        self.admin.delete_topics([self.topic])[self.topic].result(30)

    def query(self, dataset, optimized=True):
        name = "004_max_transaction_by_state.sql" if optimized else "003_max_transaction_by_state_raw.sql"
        return [tuple(row) for row in self.client.query(query_file(name),
                parameters={"database": self.db, "dataset_id": dataset}).result_rows]

    def insert(self, dataset, state, category, amount, txid, index):
        row = parse_row(next(iter_csv("examples/analytics_demo.csv"))[1], index)
        row.update(dataset_id=dataset, us_state=state, cat_id=category, amount=amount,
                   transaction_id=txid, kafka_topic=self.topic, kafka_partition=0, kafka_offset=index)
        payload = [row[name] for name in ALL_COLUMNS]
        # Raw JSON preserves Decimal exactly; direct insert triggers the same aggregate MV.
        self.client.raw_insert(f"{self.db}.transactions", column_names=list(ALL_COLUMNS),
                              insert_block=json.dumps(dict(zip(ALL_COLUMNS, payload))).encode(),
                              fmt="JSONEachRow")

    def test_actual_ddl_metadata_idempotence_and_invalid_statement(self):
        engines = dict(self.client.query("SELECT name, engine FROM system.tables WHERE database={db:String}",
                                         parameters={"db": self.db}).result_rows)
        self.assertEqual(engines["transactions"], "MergeTree")
        self.assertEqual(engines["transactions_kafka"], "Kafka")
        self.assertEqual(engines["state_max"], "AggregatingMergeTree")
        self.assertEqual(engines["kafka_to_transactions"], "MaterializedView")
        columns = {r[0]: (r[1], r[2]) for r in self.client.query(
            "SELECT name,type,compression_codec FROM system.columns WHERE database={db:String} AND table='transactions'",
            parameters={"db": self.db}).result_rows}
        self.assertEqual(columns["amount"][0], "Decimal(18, 2)")
        self.assertEqual(columns["us_state"][0], "LowCardinality(String)")
        self.assertIn("Delta", columns["transaction_time"][1])
        self.assertIn("ZSTD", columns["street"][1])
        self.insert("idempotence", "TX", "home", "12.34", "a", 1)
        execute_ddl(self.client, self.ddl)
        self.assertEqual(self.query("idempotence")[0][2], Decimal("12.34"))
        with self.assertRaises(Exception):
            execute_ddl(self.client, f"CREATE TABLE {self.db}.broken (x NotAType) ENGINE=MergeTree ORDER BY x;")
        self.assertFalse(self.client.query(f"EXISTS TABLE {self.db}.broken").first_row[0])

    def test_ties_negative_decimals_isolation_and_unmerged_blocks(self):
        self.client.command(f"SYSTEM STOP MERGES {self.db}.state_max")
        self.addCleanup(self.client.command, f"SYSTEM START MERGES {self.db}.state_max")
        self.assertEqual(self.query("empty"), [])
        cases = [("TX", "old", "1.00", "a"), ("TX", "tie_first", "2.00", "b"),
                 ("TX", "tie_last", "2.00", "z"), ("DC", "negative", "-1.00", "d"),
                 ("DC", "lower", "-10.00", "e"), ("WA", "small", "0.10", "f"),
                 ("WA", "larger", "0.20", "g")]
        for index, (state, cat, amount, txid) in enumerate(cases, 1):
            self.insert("blocks", state, cat, amount, txid, index)
        expected = [("DC", "negative", Decimal("-1.00"), "d"),
                    ("TX", "tie_last", Decimal("2.00"), "z"),
                    ("WA", "larger", Decimal("0.20"), "g")]
        self.assertEqual(self.query("blocks"), expected)
        self.assertEqual(self.query("blocks", False), expected)
        self.assertGreater(self.client.query(
            "SELECT count() FROM system.parts WHERE active AND database={db:String} AND table='state_max'",
            parameters={"db": self.db}).first_row[0], 1)
        self.insert("other", "TX", "unrelated", "999.00", "x", 1)
        self.assertEqual(self.query("blocks"), expected)
        self.insert("blocks", "TX", "new_max", "3.00", "h", 8)
        self.assertEqual(self.query("blocks")[1], ("TX", "new_max", Decimal("3.00"), "h"))

    def test_real_kafka_replay_all_columns_and_export(self):
        manifest = load_file(Path("examples/analytics_demo.csv"), broker=self.broker, topic=self.topic,
                             manifest_path=self.manifest)
        wait_dataset(self.client, manifest["dataset_id"], 9, self.db)
        first = self.query(manifest["dataset_id"])
        export = export_results(self.manifest, Path(self.temp.name) / "result.csv",
                                database=self.db, client=self.client)
        self.assertEqual(export["verified_unique_records"], 9)
        self.assertEqual(export["regions"], 5)
        load_file(Path("examples/analytics_demo.csv"), broker=self.broker, topic=self.topic,
                  manifest_path=self.manifest)
        deadline = time.monotonic() + 30
        count = 0
        while time.monotonic() < deadline:
            count = self.client.query(f"SELECT count() FROM {self.db}.transactions").first_row[0]
            if count >= 18:
                break
            time.sleep(0.2)
        self.assertGreaterEqual(count, 18)
        self.assertEqual(self.query(manifest["dataset_id"]), first)
        replay = export_results(self.manifest, Path(self.temp.name) / "replayed.csv",
                                database=self.db, client=self.client)
        self.assertEqual(replay["unique_transactions"], 9)
        self.assertEqual((Path(self.temp.name) / "result.csv").read_bytes(),
                         (Path(self.temp.name) / "replayed.csv").read_bytes())
        offsets = self.client.query(f"SELECT uniqExact(kafka_offset), uniqExact(kafka_partition), "
                                    f"uniqExact(kafka_topic) FROM {self.db}.transactions").first_row
        self.assertEqual(offsets, (18, 1, 1))

    def test_broken_json_surfaces_as_consumer_error(self):
        producer = Producer({"bootstrap.servers": self.broker, "message.timeout.ms": 10000})
        errors = []
        producer.produce(self.topic, value=b"{broken", on_delivery=lambda e, m: errors.append(e) if e else None)
        self.assertEqual(producer.flush(15), 0)
        self.assertEqual(errors, [])
        deadline = time.monotonic() + 30
        exceptions = []
        while time.monotonic() < deadline:
            rows = self.client.query("SELECT exceptions.text FROM system.kafka_consumers "
                                     "WHERE database={db:String}", parameters={"db": self.db}).result_rows
            exceptions = [message for row in rows for message in row[0]]
            if exceptions:
                break
            time.sleep(0.5)
        self.assertTrue(exceptions, "Broken JSON was not reported")
        self.assertEqual(self.client.query(f"SELECT count() FROM {self.db}.transactions").first_row[0], 0)


if __name__ == "__main__":
    unittest.main()
