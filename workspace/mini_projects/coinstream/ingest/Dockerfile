FROM python:3.11-slim
WORKDIR /app
COPY ingest/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingest /app/ingest
COPY shared /app/shared
ENV PYTHONPATH=/app
CMD ["python", "-u", "-m", "ingest.app"]
