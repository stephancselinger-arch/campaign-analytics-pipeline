FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8004
# Default command runs the ingestion gateway; the consumer worker is started
# with `python -m app.pipeline.worker` (see docker-compose.yml).
CMD ["uvicorn", "app.ingest.api:app", "--host", "0.0.0.0", "--port", "8004"]
