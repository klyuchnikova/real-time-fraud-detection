"""Provision the Grafana reader separately from training schema migrations."""
import os
import psycopg
from psycopg import sql

def main():
    password = os.environ["GRAFANA_DB_PASSWORD"]
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        if not connection.execute("SELECT 1 FROM pg_roles WHERE rolname='grafana_reader'").fetchone():
            connection.execute("CREATE ROLE grafana_reader LOGIN")
        connection.execute(sql.SQL("ALTER ROLE grafana_reader PASSWORD {}").format(sql.Literal(password)))
        connection.execute("GRANT CONNECT ON DATABASE fraud TO grafana_reader")
        connection.execute("GRANT USAGE ON SCHEMA public TO grafana_reader")
        connection.execute("GRANT SELECT ON monitoring_scores TO grafana_reader")

if __name__ == "__main__":
    main()

