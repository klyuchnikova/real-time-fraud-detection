"""Short transactional database operations; no connections at DAG import."""
import os
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row

@contextmanager
def connect():
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as connection:
        yield connection

def lock(connection):
    # Shared by supply, snapshots, retention and initialization.
    connection.execute("SELECT pg_advisory_xact_lock(730031)")

def apply_schema():
    """Read versioned SQL and apply idempotent DDL/migrations; no data is removed."""
    from pathlib import Path
    with connect() as connection:
        for path in sorted((Path(__file__).resolve().parents[1] / "sql").glob("*.sql")):
            connection.execute(path.read_text(encoding="utf-8"))
