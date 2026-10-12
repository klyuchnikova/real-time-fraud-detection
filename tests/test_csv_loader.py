"""CSV contract and producer failure tests, without running infrastructure."""
import csv
import io
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

from common.analytics import inspect_source, iter_csv, parse_row, transaction_id
from common.schema import INPUT_COLUMNS, read_transactions_csv
from scripts.load_csv_to_kafka import load_file


class FakeProducer:
    def __init__(self, config, *, error=None, pending=0):
        self.config, self.error, self.pending = config, error, pending
        self.records = []

    def produce(self, topic, *, key, value, on_delivery):
        self.records.append((topic, key, json.loads(value)))
        on_delivery(self.error, None)

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        return self.pending


class LoaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "input.csv"
        self.manifest = Path(self.temp.name) / "manifest.json"
        with Path("examples/analytics_demo.csv").open(newline="", encoding="utf-8") as source:
            self.rows = list(csv.DictReader(source))

    def write(self, rows=None, columns=INPUT_COLUMNS, bom=False):
        with self.path.open("w", newline="", encoding="utf-8-sig" if bom else "utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.rows if rows is None else rows)
        return self.path

    def load(self, factory, **kwargs):
        return load_file(self.path, broker="unused", topic="test", manifest_path=self.manifest,
                         producer_factory=factory, **kwargs)

    def test_shared_reader_quotes_bom_and_reordered_columns(self):
        self.write(columns=tuple(reversed(INPUT_COLUMNS)), bom=True)
        row = parse_row(next(iter_csv(self.path))[1], 1)
        self.assertEqual(row["amount"], "25.79")
        self.assertEqual(parse_row(list(iter_csv(self.path))[3][1], 4)["name_1"], "Demo, User")

    def test_target_is_optional_only_for_cli(self):
        rows = [dict(self.rows[0], target="1")]
        self.write(rows, (*INPUT_COLUMNS, "target"))
        self.assertEqual(parse_row(next(iter_csv(self.path))[1], 1)["target"], 1)
        with self.path.open() as source, self.assertRaises(ValueError):
            read_transactions_csv(source)

    def test_empty_missing_duplicate_and_malformed_headers(self):
        for content in ("", "amount\n1\n", ",".join((*INPUT_COLUMNS, "amount")) + "\n"):
            with self.subTest(content=content):
                self.path.write_text(content)
                with self.assertRaises(ValueError):
                    inspect_source(self.path)
        self.path.write_text(",".join(INPUT_COLUMNS) + '\n"unfinished')
        with self.assertRaises(csv.Error):
            inspect_source(self.path)

    def test_invalid_values_fail_before_producer_creation(self):
        cases = [("amount", "NaN"), ("amount", "1.001"), ("amount", "1e20"),
                 ("us_state", "XX"), ("lat", "91"), ("lon", "inf"),
                 ("population_city", "-1"), ("transaction_time", "bad"),
                 ("transaction_time", "2020-01-01T00:00:00+01:00"), ("cat_id", "")]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                self.write([dict(self.rows[0], **{field: value})])
                factory = Mock()
                with self.assertRaises(ValueError):
                    self.load(factory)
                factory.assert_not_called()
                self.assertEqual(json.loads(self.manifest.read_text())["status"], "failed")

    def test_amounts_preserve_decimal_and_negative_values(self):
        for value in ("0.10", "-1.00", "28948.90"):
            self.assertEqual(Decimal(parse_row(dict(self.rows[0], amount=value), 1)["amount"]), Decimal(value))

    def test_identity_replay_and_prefix_selection(self):
        self.write()
        full = inspect_source(self.path)
        prefix = inspect_source(self.path, 1)
        self.assertEqual(full, inspect_source(self.path))
        self.assertNotEqual(full["dataset_id"], prefix["dataset_id"])
        self.assertEqual(transaction_id(full["dataset_id"], 1), transaction_id(full["dataset_id"], 1))
        self.assertNotEqual(transaction_id(full["dataset_id"], 1), transaction_id(full["dataset_id"], 2))
        with self.assertRaises(ValueError):
            inspect_source(self.path, 0)

    def test_success_requires_all_acknowledgements_and_flat_contract(self):
        self.write()
        producer = FakeProducer({})
        manifest = self.load(lambda config: producer)
        self.assertEqual(manifest["confirmed_messages"], len(self.rows))
        payload = producer.records[0][2]
        self.assertTrue(set(INPUT_COLUMNS) <= set(payload))
        self.assertIsNone(payload["target"])
        self.assertEqual(payload["amount"], "25.79")
        self.assertEqual(json.loads(self.manifest.read_text())["status"], "success")

    def test_delivery_errors_and_pending_messages_fail_manifest(self):
        self.write()
        for producer in (FakeProducer({}, error="broker failed"), FakeProducer({}, pending=1)):
            with self.subTest(producer=producer), self.assertRaises(RuntimeError):
                self.load(lambda config: producer)
            self.assertEqual(json.loads(self.manifest.read_text())["status"], "failed")

    def test_queue_timeout_is_bounded(self):
        self.write()
        producer = FakeProducer({})
        producer.produce = Mock(side_effect=BufferError)
        with self.assertRaises(TimeoutError):
            self.load(lambda config: producer, timeout=0.001)
        self.assertEqual(json.loads(self.manifest.read_text())["status"], "failed")


if __name__ == "__main__":
    unittest.main()
