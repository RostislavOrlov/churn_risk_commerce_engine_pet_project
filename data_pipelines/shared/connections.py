import asyncpg
import clickhouse_connect
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
from confluent_kafka import Consumer
from neo4j import GraphDatabase
from redis import Redis
from data_pipelines.shared.config import settings


class SharedInfrastructure:
    def __init__(self):
        self._pg_pool = None
        self._ch_client = None
        self._es_client = None
        self._kafka_producer = None
        self._kafka_consumer = None
        self._neo4j_driver = None
        self._redis_client = None
        self._minio_client = None

    async def get_pg_pool(self) -> asyncpg.Pool:
        if not self._pg_pool:
            self._pg_pool = await asyncpg.create_pool(settings.pg_dsn, min_size=2, max_size=10)
        return self._pg_pool

    async def close_pg_pool(self):
        if self._pg_pool:
            await self._pg_pool.close()

    def get_clickhouse_client(self):
        if not self._ch_client:
            self._ch_client = clickhouse_connect.get_client(**settings.clickhouse_config)
        return self._ch_client

    def get_elasticsearch_client(self) -> Elasticsearch:
        if not self._es_client:
            self._es_client = Elasticsearch([settings.elasticsearch_url], request_timeout=60)
        return self._es_client

    async def get_kafka_producer(self) -> KafkaProducer:
        if not self._kafka_producer:
            import json
            self._kafka_producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                compression_type="gzip",
                batch_size=16384,        
                linger_ms=10,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1
            )
        return self._kafka_producer

    async def close_kafka_producer(self):
        if self._kafka_producer:
            self._kafka_producer.flush()
            self._kafka_producer.close()
            self._kafka_producer = None

    def get_kafka_consumer(self) -> Consumer:
        if not self._kafka_consumer:
            kafka_config = {
                'bootstrap.servers': settings.kafka_bootstrap_servers,
                'group.id': 'retail_click_consumers',      
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            self._kafka_consumer = Consumer(kafka_config)
        return self._kafka_consumer

    def get_neo4j_driver(self):
        if not self._neo4j_driver:
            self._neo4j_driver = GraphDatabase.driver(
                settings.neo4j_url,
                auth=(settings.neo4j_user, settings.neo4j_password),
                max_connection_pool_size=200,   
                connection_timeout=0.5         
            )
        return self._neo4j_driver

    def close_neo4j_driver(self):
        if self._neo4j_driver:
            self._neo4j_driver.close()

    def get_redis_client(self) -> Redis:
        if not self._redis_client:
            self._redis_client = Redis(
                host=settings.redis_host, 
                port=settings.redis_port, 
                decode_responses=True
            )
        return self._redis_client


infra = SharedInfrastructure()


class ConnectionFactory:
    """Алиас обратной совместимости для старых точек входа run.py."""

    def __init__(self, settings=None):
        self.settings = settings

    def get_clickhouse_client(self):
        return infra.get_clickhouse_client()

    def get_neo4j_driver(self):
        return infra.get_neo4j_driver()

    def get_elasticsearch_client(self):
        return infra.get_elasticsearch_client()

    def get_redis_client(self):
        return infra.get_redis_client()
