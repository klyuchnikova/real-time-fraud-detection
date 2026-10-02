FROM python:3.12-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY interface/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common /app/common
COPY interface /app/interface
COPY scripts /app/scripts
COPY examples /app/examples

EXPOSE 8501
CMD ["streamlit", "run", "interface/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
