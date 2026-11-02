"""Real PostgreSQL tests using a fresh private schema, never production tables."""
import csv
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from common.registry import active_version, load_bundle, atomic_json
from pipeline.db import apply_schema, connect
from pipeline.initialize import initialize
from pipeline.retrain import (reserve_window,prepare_dataset,train_candidate,evaluate_candidate,
                              save_candidate,publish_candidate,finish_month,run_training)
from pipeline.supply import reserve_batch,prepare_batch,insert_batch,prune_history
from pipeline.config import work_root,models_root
from pipeline.data import NoData
from tests.support import frame

@unittest.skipUnless(os.getenv("INTEGRATION_TESTS") == "1","Set INTEGRATION_TESTS=1 to use a real PostgreSQL private schema")
class PipelineIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.base_url = os.environ["DATABASE_URL"]
        self.schema = "test_" + uuid.uuid4().hex
        with psycopg.connect(self.base_url,autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.temporary = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "DATABASE_URL":make_conninfo(self.base_url,options=f"-c search_path={self.schema}"),
            "ARTIFACTS_ROOT":self.temporary.name,"TRAIN_ITERATIONS":"8",
            "SUPPLY_BATCH_SIZE":"20","HISTORY_MONTHS":"36"})
        self.environment.start()
        apply_schema()
        source = frame(4)
        with connect() as connection:
            for row in source.to_dict("records"):
                connection.execute("INSERT INTO labeled_transactions VALUES(%s,%s,%s,%s,%s)",
                                   (row.pop("transaction_id"),row["transaction_time"],row.pop("target"),Jsonb(row),"fixture"))
            connection.execute("INSERT INTO training_state(singleton,next_test_month) VALUES(TRUE,'2019-03-01')")

    def tearDown(self):
        self.environment.stop()
        with psycopg.connect(self.base_url,autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))
        self.temporary.cleanup()

    def state(self):
        with connect() as connection:
            return connection.execute("SELECT * FROM training_state").fetchone()

    def candidate(self,run_id):
        request=reserve_window(run_id)
        for function in (prepare_dataset,train_candidate,evaluate_candidate,save_candidate):
            request=function(request)
        return request

    def test_recover_saved_and_published_candidate_without_double_shift(self):
        initial=self.state()
        request=self.candidate("failure-before-publication")
        self.assertEqual(self.state()["next_test_month"],initial["next_test_month"])
        # A new DAG run resumes the same pending reservation.
        self.assertEqual(reserve_window("new-run-after-failure")["version"],request["version"])
        publish_candidate(request)
        self.assertEqual(active_version(models_root()),request["version"])
        self.assertEqual(self.state()["next_test_month"],initial["next_test_month"])
        # Simulate publication succeeding while its DB transaction fails.
        with connect() as connection:
            connection.execute("UPDATE training_runs SET status='saved' WHERE version=%s",(request["version"],))
        publish_candidate(request)
        finish_month(request)
        expected=self.state()["next_test_month"]
        self.assertEqual(expected.isoformat(),"2019-04-01")
        for function in (save_candidate,publish_candidate,finish_month):
            function(request)
        self.assertEqual(self.state()["next_test_month"],expected)
        self.assertIsNone(self.state()["pending_version"])
        self.assertEqual(reserve_window("new-run-after-failure")["version"],request["version"])
        self.assertEqual(self.state()["next_test_month"],expected)
        self.assertEqual(reserve_window("failure-before-publication")["version"],request["version"])

    def test_rejected_candidate_is_saved_but_not_published_and_month_advances(self):
        first=run_training("initial")
        active=active_version(models_root())
        request=reserve_window("rejected")
        prepare_dataset(request)
        train_candidate(request)
        evaluate_candidate(request)
        # Deterministic injection exercises publication behavior independently of training luck.
        path=work_root()/request["version"]/"decision.json"
        import json
        decision=json.loads(path.read_text())
        decision["accepted"]=False
        atomic_json(path,decision)
        save_candidate(request)
        publish_candidate(request)
        finish_month(request)
        self.assertEqual(active_version(models_root()),active)
        _,_,metadata=load_bundle(models_root(),request["version"])
        self.assertFalse(metadata["decision"]["accepted"])
        self.assertEqual(self.state()["next_test_month"].isoformat(),"2019-05-01")
        with self.assertRaises(NoData):
            reserve_window("missing-may")
        self.assertEqual(self.state()["next_test_month"].isoformat(),"2019-05-01")

    def test_supply_retry_does_not_duplicate_or_skip_month(self):
        batch=reserve_batch("supply-one")
        prepare_batch(batch)
        insert_batch(batch)
        insert_batch(batch)
        resumed=reserve_batch("retry-in-new-run")
        self.assertEqual(resumed,batch)
        prune_history(resumed)
        prune_history(resumed)
        self.assertEqual(reserve_batch("supply-one"),batch)
        self.assertEqual(reserve_batch("retry-in-new-run"),batch)
        with connect() as connection:
            row=connection.execute("SELECT count(*) AS n, count(DISTINCT transaction_id) AS unique_n FROM labeled_transactions WHERE batch_id=%s",(batch["batch_id"],)).fetchone()
        self.assertEqual(row,{"n":20,"unique_n":20})
        self.assertEqual(batch["target"],"2019-05-01")
        self.assertEqual(reserve_batch("supply-two")["target"],"2019-06-01")

    def test_retention_protects_unprocessed_window_then_removes_expired_rows(self):
        with patch.dict(os.environ,{"HISTORY_MONTHS":"3"}):
            batch=insert_batch(prepare_batch(reserve_batch("retention")))
            with self.assertRaisesRegex(ValueError,"unprocessed"):
                prune_history(batch)
            with connect() as connection:
                connection.execute("UPDATE training_state SET next_test_month='2019-05-01'")
            prune_history(batch)
            with connect() as connection:
                oldest=connection.execute("SELECT min(transaction_time) AS oldest FROM labeled_transactions").fetchone()["oldest"]
            self.assertEqual(oldest.month,3)

    def test_initialization_is_transactional_and_idempotent_and_migration_works(self):
        path=Path(self.temporary.name)/"train.csv"
        source=frame(3).drop(columns=["transaction_id"])
        source.to_csv(path,index=False)
        with connect() as connection:
            connection.execute("TRUNCATE labeled_transactions,training_state")
            connection.execute("ALTER TABLE transaction_scores DROP COLUMN model_version")
        with patch.dict(os.environ,{"TRAIN_DATA_CSV":str(path)}):
            initialize()
            initialize()
        with connect() as connection:
            count=connection.execute("SELECT count(*) AS n FROM labeled_transactions").fetchone()["n"]
            receipts=connection.execute("SELECT count(*) AS n FROM dataset_sources").fetchone()["n"]
            connection.execute("INSERT INTO transaction_scores(transaction_id,score,fraud_flag,model_version) VALUES('test',0.5,0,'version')")
        self.assertEqual(count,90)
        self.assertEqual(receipts,1)
        # A bad source rolls back both inserted rows and the receipt.
        with connect() as connection:
            connection.execute("TRUNCATE labeled_transactions,training_state,dataset_sources")
        source.loc[50,"target"]=2
        source.to_csv(path,index=False)
        with patch.dict(os.environ,{"TRAIN_DATA_CSV":str(path)}):
            with self.assertRaises(ValueError):
                initialize()
        with connect() as connection:
            self.assertEqual(connection.execute("SELECT count(*) AS n FROM labeled_transactions").fetchone()["n"],0)
            self.assertEqual(connection.execute("SELECT count(*) AS n FROM dataset_sources").fetchone()["n"],0)

    def test_snapshot_remains_frozen_and_invalid_classes_do_not_advance(self):
        request=reserve_window("frozen")
        path=work_root()/request["version"]/"snapshot.jsonl"
        original=path.read_bytes()
        with connect() as connection:
            connection.execute("UPDATE labeled_transactions SET data=jsonb_set(data,'{amount}','999999')")
        self.assertEqual(reserve_window("retry-frozen")["version"],request["version"])
        self.assertEqual(path.read_bytes(),original)
        prepare_dataset(request)
        # A new candidate with a single-class snapshot must fail before saving or cursor movement.
        from pipeline.data import read_frame,save_frame
        source=read_frame(path)
        source["target"]=0
        save_frame(path,source)
        before=self.state()["next_test_month"]
        with self.assertRaisesRegex(ValueError,"Data quality"):
            prepare_dataset(request)
        self.assertEqual(self.state()["next_test_month"],before)
        self.assertIsNone(active_version(models_root()))
