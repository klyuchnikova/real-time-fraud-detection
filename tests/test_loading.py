import csv
import unittest
from decimal import Decimal
from pathlib import Path
from scripts.load_source import parse_row,STATES
from common.clients import client_identity

def fixture_rows():
    with (Path(__file__).parent/"fixtures/transactions.csv").open(encoding="utf-8",newline="") as stream:
        return list(csv.DictReader(stream))

class SourceValidationTest(unittest.TestCase):
    def test_dc_is_a_federal_district_and_directory_has_no_unknown(self):
        with (Path(__file__).resolve().parents[1]/"dbt/seeds/states.csv").open(encoding="utf-8",newline="") as stream:
            rows=list(csv.DictReader(stream))
        self.assertEqual(set(STATES),{row["state_code"] for row in rows})
        self.assertEqual(len(rows),51)
        self.assertEqual(sum(row["region_type"]=="STATE" for row in rows),50)
        self.assertEqual([row for row in rows if row["region_type"]=="FEDERAL_DISTRICT"][0]["state_code"],"DC")

    def test_money_and_postal_codes_keep_precision(self):
        raw=fixture_rows()[0]
        parsed=parse_row(raw,1)
        self.assertEqual(parsed["amount"],Decimal("49.99"))
        self.assertEqual(parsed["post_code"],"00123")
        self.assertEqual(parsed["transaction_id"],1)
        self.assertEqual(client_identity({**raw,"us_state":" ny "})[4],"NY")

    def test_invalid_rows_are_rejected(self):
        base=fixture_rows()[0]
        for name,value in [("amount","-1"),("amount","NaN"),("amount","1.001"),
                           ("target","2"),("us_state","UNKNOWN"),("lat","91"),("transaction_time","wrong")]:
            with self.subTest(name=name,value=value),self.assertRaises((ValueError,TypeError)):
                parse_row({**base,name:value},1)
