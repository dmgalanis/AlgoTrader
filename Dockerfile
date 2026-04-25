FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot/trader.py .
COPY config/ ./config/

ENV PYTHONUNBUFFERED=1

CMD ["python", "bot/trader.py"]
