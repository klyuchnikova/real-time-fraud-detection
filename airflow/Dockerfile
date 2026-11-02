FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/project-requirements.txt && pip check
COPY --chown=airflow:root common /app/common
COPY --chown=airflow:root pipeline /app/pipeline
COPY --chown=airflow:root sql /app/sql
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root tests /app/tests
COPY --chown=airflow:root airflow/init.py /app/airflow_init.py
ENV PYTHONPATH=/app OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=2
