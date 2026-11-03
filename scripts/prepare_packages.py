"""Reproducible SQL alias compatibility for evaluator 1.4.1 on ClickHouse."""
from pathlib import Path
import yaml

def main():
    project=Path(__file__).resolve().parents[1]/"dbt"
    packages=yaml.safe_load((project/"packages.yml").read_text(encoding="utf-8"))["packages"]
    package=next(item for item in packages if item["package"]=="dbt-labs/dbt_project_evaluator")
    if str(package["version"])!="1.4.1":
        raise RuntimeError("Review evaluator compatibility before changing its pinned version")
    path=project/"dbt_packages/dbt_project_evaluator/models/marts/dag/fct_rejoining_of_upstream_concepts.sql"
    source=path.read_text(encoding="utf-8")
    original="        rejoined.parent,\n        rejoined.child,"
    replacement="        rejoined.parent as parent,\n        rejoined.child as child,"
    if replacement in source:
        print("Evaluator CTE aliases already explicit")
    elif source.count(original)==1:
        path.write_text(source.replace(original,replacement),encoding="utf-8",newline="\n")
        print("Prepared evaluator CTE aliases")
    else:
        raise RuntimeError("Unexpected evaluator SQL; refusing an ambiguous compatibility change")

if __name__=="__main__":
    main()
