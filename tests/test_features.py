import unittest
import numpy as np
import pandas as pd
from common.features import fit_preprocessor, prepare_features
from tests.support import frame

class FeaturesTest(unittest.TestCase):
    def setUp(self):
        self.source = frame()
        self.spec = fit_preprocessor(self.source.iloc[:60])

    def test_batch_matches_single_and_order_is_preserved(self):
        batch = prepare_features(self.source, self.spec)
        for index in (0,31,91):
            single = prepare_features(self.source.iloc[index].to_dict(), self.spec)
            pd.testing.assert_series_equal(batch.iloc[index],single.iloc[0],check_names=False)
        self.assertEqual(batch.columns.tolist(),self.spec["feature_names"])

    def test_defaults_use_train_only_and_never_target(self):
        changed = self.source.iloc[:60].copy()
        changed["target"] = 1-changed["target"]
        self.assertEqual(fit_preprocessor(changed),self.spec)
        test = self.source.iloc[60:].copy()
        test["amount"] = 10000000
        self.assertEqual(self.spec,fit_preprocessor(self.source.iloc[:60]))
        test.iloc[0,test.columns.get_loc("amount")] = None
        self.assertAlmostEqual(prepare_features(test,self.spec).iloc[0]["amount_log"],
                               np.log1p(self.spec["numeric_defaults"]["amount"]))

    def test_unknown_categories_and_missing_values(self):
        row = self.source.iloc[0].to_dict()
        row["merch"] = "never-seen"
        row["jobs"] = None
        result = prepare_features(row,self.spec)
        self.assertEqual(result.iloc[0]["merch"],"never-seen")
        self.assertEqual(result.iloc[0]["jobs"],"__MISSING__")

    def test_invalid_inputs(self):
        for field,value in (("amount",-1),("amount",float("inf")),("lat",91),("lon",-181),
                            ("merchant_lat",None),("transaction_time","wrong")):
            with self.subTest(field=field,value=value):
                row = self.source.iloc[0].to_dict()
                row[field] = value
                with self.assertRaises((ValueError,TypeError)):
                    prepare_features(row,self.spec)
        row = self.source.iloc[0].to_dict()
        row.pop("amount")
        with self.assertRaisesRegex(ValueError,"Missing fields: amount"):
            prepare_features(row,self.spec)
