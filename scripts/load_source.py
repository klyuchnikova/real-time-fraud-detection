"""Validate and publish one labeled CSV; preserve client IDs across runs and source order."""
import argparse
import hashlib
import json
import math
import os
from datetime import datetime
from decimal import Decimal,InvalidOperation
from pathlib import Path
from filelock import FileLock
from common.schema import INPUT_COLUMNS,read_transactions_csv
from common.clients import client_identity,open_registry,registry_uuid,ensure_clients
from scripts.clickhouse import connect,identifier

ROOT=Path(__file__).resolve().parents[1]
STATES=frozenset("AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY".split())
COLUMNS=("transaction_id","client_id",*INPUT_COLUMNS,"target")

def file_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream,"sha256").hexdigest()

def parse_row(row,index):
    if None in row or any(row.get(name) is None or not row[name].strip() for name in (*INPUT_COLUMNS,"target")):
        raise ValueError(f"CSV row {index}: missing fields")
    result={name:row[name] for name in INPUT_COLUMNS}
    timestamp=datetime.fromisoformat(row["transaction_time"])
    if timestamp.tzinfo is not None or timestamp.microsecond or not datetime(1970,1,1)<=timestamp<datetime(2106,2,7):
        raise ValueError(f"CSV row {index}: invalid UTC timestamp")
    result["transaction_time"]=timestamp
    try:
        amount=Decimal(row["amount"])
        if not amount.is_finite() or amount<0 or amount>=Decimal("10000000000000000") or amount.as_tuple().exponent < -2:
            raise ValueError(f"CSV row {index}: invalid amount")
        result["amount"]=amount.quantize(Decimal("0.01"))
    except InvalidOperation as error:
        raise ValueError(f"CSV row {index}: invalid amount") from error
    state=row["us_state"].strip().upper()
    if state not in STATES:
        raise ValueError(f"CSV row {index}: unknown region")
    if row["gender"].strip().upper() not in ("M","F"):
        raise ValueError(f"CSV row {index}: invalid gender")
    for name in ("lat","lon","merchant_lat","merchant_lon"):
        value=float(row[name])
        bound=90 if name.endswith("lat") else 180
        if not math.isfinite(value) or not -bound<=value<=bound:
            raise ValueError(f"CSV row {index}: invalid coordinates")
        result[name]=value
    result["population_city"]=int(row["population_city"])
    if not 0<=result["population_city"]<2**32:
        raise ValueError(f"CSV row {index}: invalid population")
    if row["target"] not in ("0","1"):
        raise ValueError(f"CSV row {index}: target must be 0 or 1")
    result["target"]=int(row["target"])
    result["transaction_id"]=index
    return result

def rows(path):
    with Path(path).open(encoding="utf-8-sig",newline="") as stream:
        reader=read_transactions_csv(stream,allow_target=True)
        if "target" not in reader.fieldnames:
            raise ValueError("A labeled CSV with target is required")
        for index,row in enumerate(reader,1):
            yield row,parse_row(row,index)

def inspect_source(path):
    fingerprint=file_hash(path)
    count=frauds=0
    amount=Decimal("0")
    identities=set()
    for raw,parsed in rows(path):
        count+=1
        frauds+=parsed["target"]
        amount+=parsed["amount"]
        identities.add(client_identity(raw))
    if not count:
        raise ValueError("Empty CSV")
    if file_hash(path)!=fingerprint:
        raise ValueError("Source changed during validation")
    return {"sha256":fingerprint,"rows":count,"frauds":frauds,"amount":amount,"identities":identities}

def load(path,registry_path,database="transactions_db",replace=False):
    """Validate before inserting; exchange complete tables. Retry rebuilds only unpublished tables.
    Clients are published first as a superset, retaining all old integer keys. The transaction
    table is replaced atomically, and a final receipt enables verified idempotent reuse.
    """
    path=Path(path)
    if not path.is_file():
        raise ValueError(f"CSV file is missing: {path}")
    database=identifier(database)
    registry_path=Path(registry_path)
    registry_path.parent.mkdir(parents=True,exist_ok=True)
    with FileLock(str(registry_path)+".lock",timeout=300):
        client=connect()
        registry=open_registry(registry_path)
        try:
            for statement in (ROOT/"sql/raw.sql").read_text(encoding="utf-8").format(database=database).split(";"):
                if statement.strip():
                    client.command(statement)
            receipt=client.query(f"SELECT * FROM {database}.ingestion_meta").named_results()
            receipt=list(receipt)
            uid=registry_uuid(registry)
            if receipt and receipt[0]["registry_uuid"]!=uid:
                raise ValueError("Client registry and ClickHouse volumes do not match; restore or reset both")
            fingerprint=file_hash(path)
            if receipt and receipt[0]["source_sha256"]!=fingerprint and not replace:
                raise ValueError("Different source: explicitly pass --replace to rebuild analytics input")
            if receipt and receipt[0]["source_sha256"]==fingerprint:
                count=client.query(f"SELECT count(),uniqExact(transaction_id),sum(target),sum(amount) FROM {database}.transactions").first_row
                lookup=client.query(f"SELECT count(),uniqExact(client_id) FROM {database}.clients").first_row
                previous=receipt[0]
                expected=(previous["source_rows"],previous["source_rows"],previous["fraud_rows"],previous["total_amount"])
                if tuple(count)==expected and lookup[0]==lookup[1]==previous["client_count"]:
                    return {"status":"unchanged","rows":previous["source_rows"],"clients":lookup[0],"sha256":fingerprint}
            profile=inspect_source(path)
            mapping=ensure_clients(registry,profile["identities"])
            for table in ("transactions","clients","ingestion_meta"):
                client.command(f"DROP TABLE IF EXISTS {database}.{table}_loading")
                client.command(f"CREATE TABLE {database}.{table}_loading AS {database}.{table}")
            batch=[]
            for raw,parsed in rows(path):
                parsed["client_id"]=mapping[client_identity(raw)]
                batch.append([parsed[column] for column in COLUMNS])
                if len(batch)>=10000:
                    client.insert(f"{database}.transactions_loading",batch,column_names=COLUMNS)
                    batch.clear()
            if batch:
                client.insert(f"{database}.transactions_loading",batch,column_names=COLUMNS)
            if file_hash(path)!=profile["sha256"]:
                raise ValueError("CSV changed during loading; published tables were not replaced")
            registry_rows=registry.execute("SELECT * FROM clients ORDER BY client_id").fetchall()
            client.insert(f"{database}.clients_loading",registry_rows,column_names=("client_id","name_1","name_2","street","one_city","us_state","post_code"))
            client.insert(f"{database}.ingestion_meta_loading",
                          [[profile["sha256"],uid,profile["rows"],profile["frauds"],profile["amount"],len(registry_rows)]],
                          column_names=("source_sha256","registry_uuid","source_rows","fraud_rows","total_amount","client_count"))
            for table in ("clients","transactions","ingestion_meta"):
                client.command(f"EXCHANGE TABLES {database}.{table} AND {database}.{table}_loading")
            for table in ("clients","transactions","ingestion_meta"):
                client.command(f"DROP TABLE {database}.{table}_loading")
            return {"status":"loaded","rows":profile["rows"],"frauds":profile["frauds"],
                    "total_amount":str(profile["amount"]),"clients":len(registry_rows),"sha256":profile["sha256"]}
        finally:
            registry.close()
            client.close()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--data",type=Path,default=os.getenv("TRAIN_DATA_CSV","/data/train.csv"))
    parser.add_argument("--registry",type=Path,default=os.getenv("CLIENT_REGISTRY","/state/clients.sqlite"))
    parser.add_argument("--database",default=os.getenv("RAW_DATABASE","transactions_db"))
    parser.add_argument("--replace",action="store_true")
    args=parser.parse_args()
    print(json.dumps(load(args.data,args.registry,args.database,args.replace)))

if __name__=="__main__":
    main()
