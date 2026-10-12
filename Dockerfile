FROM python:3.12.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY scripts/ scripts/
COPY sql/clickhouse/ sql/clickhouse/
COPY tests/ tests/
COPY examples/ examples/
COPY artifacts/preprocessor.json artifacts/preprocessor.json
CMD ["python", "-m", "scripts.check_environment"]
