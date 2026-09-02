FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY main.py signalk_access.py webapp_wsgi.py /app/

EXPOSE 8080

CMD ["python", "main.py", "--config", "/app/config.json"]
