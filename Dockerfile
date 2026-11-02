FROM python:3.11.11-slim-bookworm
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=2
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY common /app/common
COPY pipeline /app/pipeline
COPY scoring_service /app/scoring_service
COPY scores_sink /app/scores_sink
COPY scripts /app/scripts
COPY sql /app/sql
COPY tests /app/tests
COPY examples /app/examples
CMD ["python", "-m", "scripts.smoke_stack"]
