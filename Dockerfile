FROM python:3.11-slim

WORKDIR /app

# Install deps first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY trader.py ./bot/trader.py
COPY config/ ./config/

# Logs go to stdout + file
ENV PYTHONUNBUFFERED=1

CMD ["python", "bot/trader.py"]
