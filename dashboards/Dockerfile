FROM python:3.11-slim

WORKDIR /dashboard

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY credentials /dashboard/credentials

ENV GOOGLE_APPLICATION_CREDENTIALS=/dashboard/credentials/calm-sky-419511-38ce7f0f9910.json


EXPOSE 8501


CMD [
    "streamlit",
    "run",
    "ui_app.py",
    "--server.address=0.0.0.0",
    "--server.port=8501"
]