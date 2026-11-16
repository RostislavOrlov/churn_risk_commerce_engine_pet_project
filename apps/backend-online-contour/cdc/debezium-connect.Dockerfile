FROM quay.io/debezium/connect:2.7.3.Final

USER root
RUN microdnf install -y unzip || yum install -y unzip || true
COPY neo4j-sink.zip /tmp/neo4j-sink.zip
RUN mkdir -p /kafka/connect/neo4j-kafka-connect && \
    unzip -q -j /tmp/neo4j-sink.zip "*.jar" -d /kafka/connect/neo4j-kafka-connect && \
    chown -R 1001:1001 /kafka/connect/neo4j-kafka-connect && \
    rm -f /tmp/neo4j-sink.zip
USER 1001
