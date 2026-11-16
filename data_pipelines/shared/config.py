from datetime import datetime, timezone
from typing import Any
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GlobalSettings(BaseSettings):
    clickhouse_host: str = Field(default="clickhouse", validation_alias="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=8123, validation_alias="CLICKHOUSE_PORT")
    clickhouse_user: str = Field(default="clickhouse_user", validation_alias="CLICKHOUSE_USER")
    clickhouse_password: str = Field(default="clickhouse_password", validation_alias="CLICKHOUSE_PASSWORD")
    clickhouse_db: str = Field(default="retail_olap", validation_alias="CLICKHOUSE_DB")
    
    elasticsearch_url: str = Field(default="http://elasticsearch:9200", validation_alias="ELASTICSEARCH_URL")

    kafka_bootstrap_servers: str = Field(default="kafka:19092", validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    pg_dsn: str = Field(default="postgres://retail_user:retail_password@postgres-master:5432/retail_oltp", validation_alias="PG_DSN")
    
    neo4j_url: str = Field(default="bolt://neo4j:7687", validation_alias="NEO4J_URL")
    neo4j_user: str = Field(default="neo4j", validation_alias="NEO4J_USER")
    neo4j_password: str = Field(default="secure_graph_pass_99", validation_alias="NEO4J_PASSWORD")
    
    redis_host: str = Field(default="redis", validation_alias="REDIS_HOST")
    redis_port: int = Field(default=6379, validation_alias="REDIS_PORT")
    
    minio_host: str = Field(default="localhost:9000", validation_alias="MINIO_HOST")
    minio_access_key: str = Field(default="minio_admin", validation_alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minio_password_123", validation_alias="MINIO_SECRET_KEY")

    PROMETHEUS_PUSHGATEWAY_URL: str = Field(
        default="http://pushgateway:9091",
        description="URL Prometheus Pushgateway для отправки метрик батч-пайплайнов"
    )


    business_start_date: datetime = datetime(2026, 10, 23, 23, 0, 0, tzinfo=timezone.utc)

    spark_checkpoint_location: str = Field(
        default="/opt/spark/work-dir/checkpoints/funnel", 
        validation_alias="SPARK_CHECKPOINT_LOCATION"
    )

    catboost_model_path: str = Field(
        default="/opt/models/user_churn_retail_v2.cbm", 
        validation_alias="CATBOOST_MODEL_PATH"
    )

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8", 
        extra="ignore"
    )

    @property
    def clickhouse_config(self) -> dict:
        """Хелпер конфигурации для клиента clickhouse_connect."""
        return {
            "host": self.clickhouse_host,
            "port": self.clickhouse_port,
            "username": self.clickhouse_user,
            "password": self.clickhouse_password,
            "database": self.clickhouse_db,
        }

    @property
    def spark_postgres_jdbc_url(self) -> str:
        clean_host_port_db = self.pg_dsn.split("@")[-1]
        return f"jdbc:postgresql://{clean_host_port_db}"

    @property
    def spark_clickhouse_jdbc_url(self) -> str:
        return f"jdbc:ch://{self.clickhouse_host}:{self.clickhouse_port}/{self.clickhouse_db}"

    def get(self, key: str, default: Any = None) -> Any:
        """Обратная совместимость для старых репозиториев, обращающихся к настройкам как к dict."""
        return getattr(self, key, getattr(self, key.lower(), default))


settings = GlobalSettings()
