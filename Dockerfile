FROM python:3.11-slim

WORKDIR /app

# Install deps first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source — trader.py is at repo root
COPY trader.py .
COPY config/ ./config/

# Create logs directory
RUN mkdir -p /app/logs

# Logs go to stdout + file
ENV PYTHONUNBUFFERED=1

CMD ["python", "trader.py"]
