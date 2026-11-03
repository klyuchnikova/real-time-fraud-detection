import csv
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
from common.clients import open_registry
from scripts.clickhouse import connect
from scripts.load_source import load
from tests.test_loading import fixture_rows

@unittest.skipUnless(os.getenv("INTEGRATION_TESTS")=="1","Set INTEGRATION_TESTS=1 to use a private ClickHouse database")
class LoaderIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.path=Path(self.directory.name)/"source.csv"
        self.registry=Path(self.directory.name)/"clients.sqlite"
        self.database="loader_test_"+uuid.uuid4().hex
        self.write(fixture_rows())

    def tearDown(self):
        client=connect()
        client.command(f"DROP DATABASE IF EXISTS {self.database} SYNC")
        client.close()
        self.directory.cleanup()

    def write(self,rows):
        with self.path.open("w",encoding="utf-8",newline="") as stream:
            writer=csv.DictWriter(stream,fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def counts(self):
        client=connect()
        result=tuple(client.query(f"SELECT count(),uniqExact(transaction_id),sum(target),sum(amount) FROM {self.database}.transactions").first_row)
        client.close()
        return result

    def keys(self):
        connection=open_registry(self.registry)
        result={tuple(row[1:]):row[0] for row in connection.execute("SELECT * FROM clients")}
        connection.close()
        return result

    def test_reimport_and_reorder_keep_integer_client_ids(self):
        initial=load(self.path,self.registry,self.database)
        self.assertEqual(initial["status"],"loaded")
        keys=self.keys()
        counts=self.counts()
        self.assertEqual(load(self.path,self.registry,self.database)["status"],"unchanged")
        self.assertEqual(self.counts(),counts)
        self.write(list(reversed(fixture_rows())))
        with self.assertRaisesRegex(ValueError,"Different source"):
            load(self.path,self.registry,self.database)
        load(self.path,self.registry,self.database,replace=True)
        self.assertEqual(self.keys(),keys)
        self.assertEqual(self.counts(),counts)

    def test_invalid_source_does_not_replace_valid_tables(self):
        load(self.path,self.registry,self.database)
        counts=self.counts()
        rows=fixture_rows()
        rows[-1]["amount"]="-1"
        self.write(rows)
        with self.assertRaises(ValueError):
            load(self.path,self.registry,self.database,replace=True)
        self.assertEqual(self.counts(),counts)

    def test_failed_insert_and_retry_preserve_published_rows_and_ids(self):
        load(self.path,self.registry,self.database)
        counts=self.counts()
        keys=self.keys()
        rows=fixture_rows()
        rows.append({**rows[0],"name_1":"New","street":"99 New"})
        self.write(rows)
        client=connect()
        original_insert=client.insert
        def insert_then_fail(table,*args,**kwargs):
            result=original_insert(table,*args,**kwargs)
            if table.endswith(".transactions_loading"):
                raise RuntimeError("Injected failure after insert, before publication")
            return result
        with patch("scripts.load_source.connect",return_value=client),patch.object(client,"insert",side_effect=insert_then_fail):
            with self.assertRaisesRegex(RuntimeError,"Injected"):
                load(self.path,self.registry,self.database,replace=True)
        self.assertEqual(self.counts(),counts)
        allocated=self.keys()
        self.assertEqual({key:allocated[key] for key in keys},keys)
        load(self.path,self.registry,self.database,replace=True)
        self.assertEqual(self.keys(),allocated)
        self.assertEqual(self.counts()[0],len(rows))

    def test_mismatched_registry_is_rejected(self):
        load(self.path,self.registry,self.database)
        with self.assertRaisesRegex(ValueError,"do not match"):
            load(self.path,Path(self.directory.name)/"other.sqlite",self.database)
