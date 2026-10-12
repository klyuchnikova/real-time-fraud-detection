"""Validated CSV contract for homework 2; no ML dependencies."""
import hashlib
import io
import json
import math
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from common.schema import INPUT_COLUMNS, read_transactions_csv

STATES = frozenset("AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY".split())
FLOAT_COLUMNS = ("lat", "lon", "merchant_lat", "merchant_lon")
BUSINESS_COLUMNS = ("row_index", "transaction_id", *INPUT_COLUMNS, "target")
ALL_COLUMNS = ("dataset_id", *BUSINESS_COLUMNS, "kafka_topic", "kafka_partition", "kafka_offset")


def file_hash(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def transaction_id(dataset_id, row_index):
    return hashlib.sha256(f"{dataset_id}:{row_index}".encode()).hexdigest()


def parse_row(row, row_index):
    def fail(field, reason):
        raise ValueError(f"CSV record {row_index}, field {field}: {reason}")

    if None in row or any(value is None for value in row.values()):
        fail("columns", "wrong number of cells")
    for field in INPUT_COLUMNS:
        if not row[field].strip():
            fail(field, "empty value")
    result = {field: row[field] for field in INPUT_COLUMNS}
    result["us_state"] = row["us_state"].strip().upper()
    if result["us_state"] not in STATES:
        fail("us_state", "unknown region")
    result["cat_id"] = row["cat_id"].strip()
    result["gender"] = row["gender"].strip().upper()
    if result["gender"] not in {"M", "F"}:
        fail("gender", "expected M or F")
    try:
        amount = Decimal(row["amount"])
        if not amount.is_finite() or abs(amount) >= Decimal("10000000000000000"):
            fail("amount", "outside Decimal64(2) range or nonfinite")
        if amount.as_tuple().exponent < -2:
            fail("amount", "more than two decimal places")
        result["amount"] = format(amount.quantize(Decimal("0.01")), "f")
    except InvalidOperation:
        fail("amount", "invalid decimal")
    try:
        timestamp = datetime.fromisoformat(row["transaction_time"])
    except ValueError:
        fail("transaction_time", "invalid ISO date/time")
    if timestamp.tzinfo is not None or timestamp.microsecond:
        fail("transaction_time", "expected whole seconds without timezone")
    if not datetime(1970, 1, 1) <= timestamp <= datetime(2106, 2, 7, 6, 28, 15):
        fail("transaction_time", "outside DateTime range")
    result["transaction_time"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    try:
        population = int(row["population_city"])
    except ValueError:
        fail("population_city", "expected integer")
    if not 0 <= population <= 2**32 - 1:
        fail("population_city", "outside UInt32 range")
    result["population_city"] = population
    for field in FLOAT_COLUMNS:
        try:
            value = float(row[field])
        except ValueError:
            fail(field, "expected number")
        bound = 90 if field.endswith("lat") else 180
        if not math.isfinite(value) or not -bound <= value <= bound:
            fail(field, "invalid coordinate")
        result[field] = value
    target = row.get("target")
    if target is not None and target not in {"0", "1"}:
        fail("target", "expected 0 or 1")
    result["target"] = int(target) if target is not None else None
    result["row_index"] = row_index
    return result


def iter_csv(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as source:
        reader = read_transactions_csv(source, allow_target=True)
        yield from enumerate(reader, start=1)


class HashingReader(io.RawIOBase):
    def __init__(self, source, digest):
        super().__init__()
        self.source, self.digest = source, digest

    def readable(self):
        return True

    def readinto(self, buffer):
        chunk = self.source.read(len(buffer))
        self.digest.update(chunk)
        buffer[:len(chunk)] = chunk
        return len(chunk)


def inspect_source(path, limit=None):
    """First CSV pass validates selection and hashes every byte, including BOM."""
    if limit is not None and limit < 1:
        raise ValueError("--limit must be positive")
    path = Path(path)
    signature = (path.stat().st_size, path.stat().st_mtime_ns)
    digest = hashlib.sha256()
    selected = total = 0
    with path.open("rb") as binary:
        with io.TextIOWrapper(io.BufferedReader(HashingReader(binary, digest)), encoding="utf-8-sig", newline="") as source:
            reader = read_transactions_csv(source, allow_target=True)
            for index, row in enumerate(reader, 1):
                total = index
                if limit is None or selected < limit:
                    parse_row(row, index)
                    selected += 1
    if not selected:
        raise ValueError("CSV has no transactions")
    if signature != (path.stat().st_size, path.stat().st_mtime_ns):
        raise ValueError("CSV changed while validating")
    sha = digest.hexdigest()
    identity = json.dumps({"schema_version": 1, "sha256": sha, "selected_rows": selected}, sort_keys=True, separators=(",", ":"))
    return {
        "schema_version": 1, "source_file": str(path), "source_sha256": sha,
        "source_total_rows": total, "selected_rows": selected,
        "dataset_id": hashlib.sha256(identity.encode()).hexdigest(),
    }


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
