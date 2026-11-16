FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bash \
    gcc \
    python3-dev \
    && curl -sfL https://clickhouse.com -o /usr/local/bin/clickhouse-client \
    && chmod +x /usr/local/bin/clickhouse-client \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    pandas==2.1.1 \
    numpy==1.26.0 \
    psycopg2-binary==2.9.9 \
    asyncpg==0.28.0 \
    SQLAlchemy==2.0.21 \
    clickhouse-connect==0.6.8 \
    neo4j==5.12.0 \
    elasticsearch==8.12.0 \
    kafka-python-ng==2.2.2 \
    confluent-kafka==2.3.0 \
    redis==5.0.1 \
    pydantic==2.4.2 \
    pydantic-settings==2.0.3

WORKDIR /opt/pipelines
