"""Run the real Streamlit page without a browser; requires at least one saved score."""
import importlib.util
import os
import unittest

@unittest.skipUnless(importlib.util.find_spec("streamlit") and os.getenv("INTERFACE_TESTS")=="1",
                     "Run in interface with INTERFACE_TESTS=1 after the smoke test")
class InterfaceTest(unittest.TestCase):
    def test_results_display_actual_model_version(self):
        from streamlit.testing.v1 import AppTest
        page=AppTest.from_file("/app/interface/app.py",default_timeout=30).run()
        self.assertFalse(list(page.exception))
        button=next(item for item in page.button if item.label=="Посмотреть результаты")
        button.click().run()
        self.assertFalse(list(page.exception))
        self.assertGreater(len(page.dataframe),0)
        table=page.dataframe[0].value
        self.assertIn("model_version",table.columns)
        self.assertFalse(table.empty)
        self.assertTrue(table["model_version"].astype(str).str.startswith("v-").all())
