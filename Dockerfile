FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY trader__15_.py ./trader.py
COPY config/ ./config/

RUN mkdir -p /app/logs

ENV PYTHONUNBUFFERED=1

CMD ["python", "trader.py"]
