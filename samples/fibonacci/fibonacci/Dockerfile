FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN pip install --no-cache-dir Flask==3.1.0 gunicorn==23.0.0

COPY app.py .

USER 65532:65532
EXPOSE 8080

CMD ["gunicorn", "-b", "0.0.0.0:8080", "--threads", "4", "app:app"]
