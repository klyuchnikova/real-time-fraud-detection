FROM python:3.12-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY scores_sink/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY scores_sink /app/scores_sink

CMD ["python", "-m", "scores_sink.app"]
