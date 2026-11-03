"""Run loader tests and persist their actual result for the validation report."""
import json
import os
import unittest
from pathlib import Path

def main():
    root=Path(__file__).resolve().parents[1]
    os.environ["INTEGRATION_TESTS"]="1"
    suite=unittest.defaultTestLoader.discover(str(root/"tests"),top_level_dir=str(root))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    summary={"tests":result.testsRun,"failures":len(result.failures),"errors":len(result.errors),
             "skipped":len(result.skipped),"passed":result.testsRun-len(result.failures)-len(result.errors)-len(result.skipped)}
    path=root/"reports/loader_tests.json"
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(summary,indent=2)+"\n",encoding="utf-8")
    if not result.wasSuccessful() or result.skipped:
        raise SystemExit(1)

if __name__=="__main__":
    main()
