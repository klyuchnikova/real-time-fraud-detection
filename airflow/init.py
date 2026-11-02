"""Idempotent metadata migration and admin user creation without masking failures."""
import os
import subprocess
subprocess.run(["airflow","db","migrate"], check=True)
from airflow.www.app import create_app
with create_app().app_context() as context:
    # Use the current Flask application; never hide a failed CLI create command with '|| true'.
    from flask import current_app
    manager = current_app.appbuilder.sm
    username = os.getenv("AIRFLOW_ADMIN_USER","admin")
    if manager.find_user(username=username) is None:
        user = manager.add_user(username=username, first_name="Local", last_name="Admin",
                                email="admin@example.test", role=manager.find_role("Admin"),
                                password=os.getenv("AIRFLOW_ADMIN_PASSWORD","admin"))
        if not user:
            raise RuntimeError("Airflow admin creation failed")
