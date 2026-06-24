FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY credentials /app/credentials

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/calm-sky-419511-38ce7f0f9910.json

CMD ["python", "main.py"]

