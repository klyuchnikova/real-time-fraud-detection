"""Persistent integer client identities with database-enforced uniqueness."""
import sqlite3
import uuid
from pathlib import Path

IDENTITY_FIELDS=("name_1","name_2","street","one_city","us_state","post_code")

def client_identity(row):
    values=tuple(str(row[name]).strip() for name in IDENTITY_FIELDS)
    if any(not value for value in values):
        raise ValueError("Client identity fields must be nonempty")
    return values[:4]+(values[4].upper(),)+values[5:]

def open_registry(path):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    connection=sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript("""
    CREATE TABLE IF NOT EXISTS registry_meta (registry_uuid TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS clients (
        client_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name_1 TEXT NOT NULL, name_2 TEXT NOT NULL, street TEXT NOT NULL,
        one_city TEXT NOT NULL, us_state TEXT NOT NULL, post_code TEXT NOT NULL,
        UNIQUE(name_1,name_2,street,one_city,us_state,post_code)
    );
    """)
    if connection.execute("SELECT count(*) FROM registry_meta").fetchone()[0]==0:
        connection.execute("INSERT INTO registry_meta VALUES(?)",(str(uuid.uuid4()),))
        connection.commit()
    return connection

def registry_uuid(connection):
    return connection.execute("SELECT registry_uuid FROM registry_meta").fetchone()[0]

def ensure_clients(connection,identities):
    """Commit keys before raw publication, so failed imports never recycle allocated IDs."""
    mapping={tuple(row[1:]):row[0] for row in connection.execute("SELECT * FROM clients")}
    for identity in sorted(set(identities)-set(mapping)):
        cursor=connection.execute(
            "INSERT INTO clients(name_1,name_2,street,one_city,us_state,post_code) VALUES(?,?,?,?,?,?)",identity)
        mapping[identity]=cursor.lastrowid
    connection.commit()
    return mapping
