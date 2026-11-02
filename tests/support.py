"""Small deterministic labeled fixture, separate from production data."""
import csv
from pathlib import Path
import pandas as pd
from catboost import CatBoostClassifier
from common.features import fit_preprocessor, prepare_features, CATEGORIES
from common.registry import save_bundle

def frame(months=4, rows_per_month=30):
    with (Path(__file__).resolve().parents[1] / "examples/demo.csv").open(encoding="utf-8") as stream:
        base = next(csv.DictReader(stream))
    rows = []
    for month in range(1, months+1):
        for index in range(rows_per_month):
            target = int(index % 3 == 0)
            row = {**base, "transaction_time":f"2019-{month:02d}-{index%28+1:02d} 12:30:00",
                   "amount": str(500+index if target else 5+index), "target":target,
                   "transaction_id":f"m{month}-r{index}"}
            rows.append(row)
    return pd.DataFrame(rows)

def bundle(root, version="v-test", source=None):
    source = frame(3) if source is None else source
    spec = fit_preprocessor(source)
    model = CatBoostClassifier(iterations=8, depth=3, random_seed=42, verbose=False,
                               thread_count=1, allow_writing_files=False)
    model.fit(prepare_features(source, spec), source["target"], cat_features=CATEGORIES)
    temporary = Path(root) / f"{version}.bin"
    temporary.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(temporary))
    save_bundle(root, version, temporary, spec, {"decision":{"accepted":True}})
    temporary.unlink()
    return spec
