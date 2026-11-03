FROM python:3.11.11-slim-bookworm
WORKDIR /workspace
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/workspace DBT_PROFILES_DIR=/workspace/dbt
RUN apt-get update && apt-get install -y --no-install-recommends make ca-certificates && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && pip check
COPY common /workspace/common
COPY scripts /workspace/scripts
COPY sql /workspace/sql
COPY tests /workspace/tests
COPY dbt /workspace/dbt
COPY Makefile /workspace/Makefile
CMD ["make", "all", "IN_CONTAINER=1"]
