"""Exercise the exact SQL shipped to Grafana in a private PostgreSQL schema."""
import json
import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from prometheus_client import CollectorRegistry
from scores_sink.app import save_message
from scores_sink.metrics import SinkMetrics

@unittest.skipUnless(os.getenv("INTEGRATION_TESTS")=="1", "Requires PostgreSQL")
class MonitoringSQLTest(unittest.TestCase):
    def setUp(self):
        self.schema="monitoring_test_"+uuid.uuid4().hex
        self.base=os.environ["DATABASE_URL"]
        with psycopg.connect(self.base,autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.connection=psycopg.connect(make_conninfo(self.base,options=f"-c search_path={self.schema}"))
        root=Path(__file__).resolve().parents[2]
        for name in ("001_transaction_scores.sql","003_monitoring.sql"):
            self.connection.execute((root/"sql"/name).read_text())
        self.connection.commit()
        self.panels=json.loads((root/"monitoring/grafana/dashboards/business.json").read_text())["panels"]
        self.now=datetime(2026,1,1,tzinfo=timezone.utc)
        self.registry=CollectorRegistry()
        self.metrics=SinkMetrics(self.registry)

    def tearDown(self):
        self.connection.close()
        with psycopg.connect(self.base,autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def query(self,panel,states,merchants):
        def quoted(values):
            return ",".join("'"+x.replace("'","''")+"'" for x in values)
        query=self.panels[panel-1]["targets"][0]["rawSql"]
        start=int((self.now-timedelta(seconds=10)).timestamp())
        end=int((self.now+timedelta(seconds=1100)).timestamp())
        query=query.replace("${state:sqlstring}",quoted(states)).replace("${merchant:sqlstring}",quoted(merchants))
        query=query.replace("$__timeFilter(scored_at)",f"scored_at BETWEEN to_timestamp({start}) AND to_timestamp({end})")
        query=query.replace("$__unixEpochFrom()",str(start)).replace("$__unixEpochTo()",str(end))
        return self.connection.execute(query).fetchall()

    def test_order_retry_and_commit_failure(self):
        score=("retry",.8,1,"v-one")
        context=("retry","DC","O'Reilly","travel",100)
        self.assertTrue(save_message(self.connection,"score",score,self.now,self.metrics))
        self.assertEqual(self.connection.execute("SELECT count(*) FROM monitoring_scores WHERE NOT has_context").fetchone()[0],1)
        self.assertTrue(save_message(self.connection,"context",context,self.now,self.metrics))
        self.assertFalse(save_message(self.connection,"score",score,self.now,self.metrics))
        self.assertFalse(save_message(self.connection,"context",context,self.now,self.metrics))
        self.assertEqual(self.connection.execute("SELECT count(*) FROM monitoring_scores WHERE has_context").fetchone()[0],1)
        with self.assertRaises(psycopg.Error):
            save_message(self.connection,"score",("bad",2,0,"v-one"),self.now,self.metrics)
        self.assertEqual(self.registry.get_sample_value("sink_scores_written_total"),1)
        self.assertEqual(self.registry.get_sample_value("sink_write_errors_total"),1)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM transaction_scores").fetchone()[0],1)

    def test_actual_dashboard_queries(self):
        expected=[]
        for i in range(1006):
            state="DC" if i%2 else "NY"
            merchant="O'Reilly" if i%3 else "Shop B"
            category="travel" if i%5 else "food"
            score=(i%21)/20
            flag=int(score>.9)
            record={"state":state,"merchant":merchant,"category":category,"score":score,"flag":flag,"amount":i+1,"version":"v-one" if i%2 else "v-two"}
            expected.append(record)
            save_message(self.connection,"context",(str(i),state,merchant,category,i+1),self.now,self.metrics)
            save_message(self.connection,"score",(str(i),score,flag,record["version"]),self.now+timedelta(seconds=i),self.metrics)
        for states,merchants in [(["DC"],["O'Reilly"]),(["DC","NY"],["O'Reilly","Shop B"]),(["__all_values__"],["__all_values__"]),(["ZZ"],["Shop B"])]:
            def matches(r):
                return ("__all_values__" in states or r["state"] in states) and ("__all_values__" in merchants or r["merchant"] in merchants)
            selected=[r for r in expected if matches(r)]
            latest=[r for r in expected[-1000:] if matches(r)]
            density=self.query(1,states,merchants)
            self.assertEqual(len(density),20)
            if selected:
                self.assertAlmostEqual(sum(r[1]*.05 for r in density),1)
                bins=Counter(min(19,int(r["score"]*20)) for r in selected)
                for b,row in enumerate(density):
                    self.assertAlmostEqual(row[1], bins[b]/(len(selected)*.05))
            else:
                self.assertTrue(all(r[1] is None for r in density))
            self.assertAlmostEqual(float(sum(r[1]*10 for r in self.query(2,states,merchants))),len(selected))
            categories=self.query(3,states,merchants)
            for category,share,count in categories:
                group=[r for r in latest if r["category"]==category]
                self.assertEqual(count,len(group))
                self.assertAlmostEqual(share,sum(r["flag"] for r in group)/len(group))
            self.assertEqual(self.query(4,states,merchants)[0][0],len(latest))
            for category,amount in self.query(5,states,merchants):
                group=[r for r in selected if r["category"]==category and r["flag"]]
                self.assertEqual(amount, sum(r["amount"] for r in group))
            versions=self.query(6,states,merchants)
            self.assertEqual(sum(r[1] for r in versions),len(selected))

