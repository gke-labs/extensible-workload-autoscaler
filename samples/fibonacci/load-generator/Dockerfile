FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN pip install --no-cache-dir aiohttp==3.11.11 schedule==1.2.2

COPY app.py .

USER 65532:65532

CMD ["python", "app.py"]
