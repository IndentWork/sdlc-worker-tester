FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir \
    azure-identity \
    azure-servicebus \
    azure-storage-blob

# Copy application code
COPY app/ ./app/

CMD ["python", "-m", "app.main"]
