import os
from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    ENV: Literal["development", "staging", "production"] = Field(default="production")
    PROJECT_NAME: str = Field(default="RealTimeCommerce-Online-Engine")
    API_V1_STR: str = Field(default="/api/v1")

    HTTP_HOST: str = Field(default="0.0.0.0")
    HTTP_PORT: int = Field(default=8080)
    WORKERS_COUNT: int = Field(default=4)

    # PostgreSQL Read/Write Splitting (SQLAlchemy asyncpg)
    POSTGRES_MASTER_DSN: str = Field(
        default="postgresql+asyncpg://retail_user:retail_password@postgres-master:5432/retail_oltp"
    )
    POSTGRES_REPLICA_DSN: str = Field(
        default="postgresql+asyncpg://retail_user:retail_password@postgres-replica-1:5433/retail_oltp"
    )
    POSTGRES_MIN_POOL_SIZE: int = Field(default=10)
    POSTGRES_MAX_POOL_SIZE: int = Field(default=40)
    POSTGRES_TIMEOUT_SEC: float = Field(default=1.5)

    # Redis Online Feature Store & Serving Layer
    REDIS_URL: str = Field(default="redis://redis:6379/0")
    REDIS_MAX_CONNECTIONS: int = Field(default=100)
    REDIS_TIMEOUT_SEC: float = Field(default=0.15)

    # Neo4j Logistics Graph
    NEO4J_URI: str = Field(default="bolt://neo4j-core:7687")
    NEO4J_USER: str = Field(default="neo4j")
    NEO4J_PASSWORD: str = Field(default="secure_retail_password")
    NEO4J_TIMEOUT_SEC: float = Field(default=0.25)

    # Keycloak 
    KEYCLOAK_SERVER_URL: str = Field(default="http://keycloak-auth:8080")
    KEYCLOAK_REALM: str = Field(default="retail-tech")
    KEYCLOAK_CLIENT_ID: str = Field(default="recommendation-api-service")
    JWT_ALGORITHM: str = Field(default="RS256")
    AUTH_ENABLED: bool = Field(default=True)
    KEYCLOAK_CLIENT_SECRET: str = Field(default="change-me-keycloak-client-secret")
    KEYCLOAK_ALLOWED_AZP: list[str] = Field(
        default=[
            "recommendation-api-service",
            "control-tower-web",
            "crm-callcenter-client",
            "online-routing-engine-client",
        ]
    )
    KEYCLOAK_VERIFY_AUDIENCE: bool = Field(default=False)
    KEYCLOAK_TIMEOUT_SEC: float = Field(default=1.5)

    # Cache, Idempotency & Load Shedding
    CACHE_L1_TTL_SEC: int = Field(default=3)
    CACHE_L2_TTL_SEC: int = Field(default=30)
    IDEMPOTENCY_TTL_SEC: int = Field(default=86400)  
    MAX_CONCURRENT_REQUESTS: int = Field(default=250)  
    READINESS_CACHE_TTL_SEC: float = Field(default=2.0)

    # Celery Background Workers
    CELERY_BROKER_URL: str = Field(default="redis://redis:6379/1")
    CELERY_RESULT_BACKEND: str = Field(default="redis://redis:6379/2")
    TMS_WEBHOOK_URL: str = Field(default="http://tms-routing-core:8080/api/v1/routes/reroute-trigger")

    # ML Model & Action Engine Thresholds
    CATBOOST_MODEL_PATH: str = Field(
        default=os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                ".models",
                "user_churn_retail_v2.cbm",
            )
        )
    )
    CATBOOST_SHADOW_MODEL_PATH: str = Field(
        default=os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                ".models",
                "user_churn_retail_challenger.cbm",
            )
        )
    )
    CHURN_RISK_THRESHOLD: float = Field(default=0.75)
    VIP_LTV_THRESHOLD_RUB: float = Field(default=50000.0)
    MAX_VOUCHER_COMPENSATION_RUB: float = Field(default=500.0)
    EGROCERY_MAX_DELAY_MINUTES: float = Field(default=30.0)

    # Circuit Breaker
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = Field(default=5)
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SEC: float = Field(default=10.0)


settings = AppSettings()
