FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

RUN useradd --create-home --no-log-init --uid 1000 monitor \
    && mkdir -p /data && chown monitor:monitor /data

USER monitor

ENV PYTHONPATH=/app

ENTRYPOINT ["kopf", "run", "--standalone", "--all-namespaces", "src/handlers/__init__.py"]
