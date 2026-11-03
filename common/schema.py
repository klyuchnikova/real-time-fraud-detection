"""CSV column contract for labeled transaction loading and validation."""
import csv

INPUT_COLUMNS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon",
)


def read_transactions_csv(source, *, allow_target=False):
    """Validate the CSV schema without loading all rows into memory."""
    reader = csv.DictReader(source, strict=True)
    columns = reader.fieldnames
    expected = set(INPUT_COLUMNS)
    allowed = (expected, expected | {"target"}) if allow_target else (expected,)
    if not columns or len(columns) != len(set(columns)) or set(columns) not in allowed:
        suffix = " (optionally plus target)" if allow_target else ""
        raise ValueError(f"CSV must contain the 17 test.csv columns{suffix}")
    return reader
