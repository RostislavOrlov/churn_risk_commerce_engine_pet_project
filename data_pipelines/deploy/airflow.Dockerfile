FROM apache/airflow:2.7.1-python3.8

USER root
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir \
    "protobuf<5" \
    "pyspark==3.4.1" \
    pydantic-settings \
    "clickhouse-connect==0.7.19" \
    confluent-kafka \
    kafka-python-ng \
    neo4j \
    elasticsearch \
    asyncpg \
    psycopg2-binary \
    redis \
    "dbt-core==1.7.18" \
    "dbt-clickhouse==1.7.7" \
    "apache-airflow-providers-apache-spark==4.1.5"
