from typing import AsyncGenerator
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
from neo4j import AsyncDriver

from ..core.cache import HighLoadCacheEngine
from ..core.circuit_breaker import HighAvailabilityCircuitBreaker
from ..repositories.postgres_client import pg_manager, PostgresAnalyticsRepository
from ..repositories.redis_client import RedisFeatureRepository
from ..repositories.neo4j_client import Neo4jGraphRepository
from ..services.analytics import ml_serving_service, ChurnModelServingService
from ..services.action_engine import ActionEngineService
from ..services.logistics_ops import LogisticsOperationsService
from ..services.topology_health import TopologyHealthService

global_cache_engine = HighLoadCacheEngine()

redis_breaker = HighAvailabilityCircuitBreaker(name="Redis-FeatureStore", failure_threshold=5, recovery_timeout=10.0)
neo4j_breaker = HighAvailabilityCircuitBreaker(name="Neo4j-LogisticsGraph", failure_threshold=3, recovery_timeout=15.0)
postgres_breaker = HighAvailabilityCircuitBreaker(name="Postgres-RWMarts", failure_threshold=5, recovery_timeout=10.0)


async def get_master_session() -> AsyncGenerator[AsyncSession, None]:
    if not pg_manager.master_session_factory:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Postgres Master pool not initialized"
        )
    async with pg_manager.master_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_replica_session() -> AsyncGenerator[AsyncSession, None]:
    if not pg_manager.replica_session_factory:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Postgres Replica pool not initialized"
        )
    async with pg_manager.replica_session_factory() as session:
        yield session


async def get_redis_client() -> AsyncGenerator[Redis, None]:
    from ..main import db_state

    if not db_state.redis_pool:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Redis pool not initialized")
    client = Redis(connection_pool=db_state.redis_pool)
    try:
        yield client
    finally:
        await client.aclose()


async def get_neo4j_driver() -> AsyncDriver:
    from ..main import db_state

    if not db_state.neo4j_driver:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Neo4j driver not initialized")
    return db_state.neo4j_driver


async def get_cache_engine(redis_client: Redis = Depends(get_redis_client)) -> HighLoadCacheEngine:
    global_cache_engine.redis = redis_client
    return global_cache_engine


async def get_postgres_repository() -> PostgresAnalyticsRepository:
    return PostgresAnalyticsRepository()


async def get_redis_repository(redis_client: Redis = Depends(get_redis_client)) -> RedisFeatureRepository:
    return RedisFeatureRepository(redis_client=redis_client)


async def get_neo4j_repository(driver: AsyncDriver = Depends(get_neo4j_driver)) -> Neo4jGraphRepository:
    return Neo4jGraphRepository(driver=driver)


async def get_analytics_service() -> ChurnModelServingService:
    return ml_serving_service


async def get_action_engine_service(
    pg_repo: PostgresAnalyticsRepository = Depends(get_postgres_repository),
    redis_repo: RedisFeatureRepository = Depends(get_redis_repository),
    neo4j_repo: Neo4jGraphRepository = Depends(get_neo4j_repository),
    ml_service: ChurnModelServingService = Depends(get_analytics_service),
) -> ActionEngineService:
    return ActionEngineService(
        postgres_repo=pg_repo,
        redis_repo=redis_repo,
        neo4j_repo=neo4j_repo,
        ml_service=ml_service,
        redis_breaker=redis_breaker,
        neo4j_breaker=neo4j_breaker,
        postgres_breaker=postgres_breaker,
    )


async def get_logistics_ops_service(
    pg_repo: PostgresAnalyticsRepository = Depends(get_postgres_repository),
    redis_repo: RedisFeatureRepository = Depends(get_redis_repository),
) -> LogisticsOperationsService:
    return LogisticsOperationsService(
        postgres_repo=pg_repo,
        redis_repo=redis_repo,
        postgres_breaker=postgres_breaker,
        redis_breaker=redis_breaker,
    )


async def get_topology_health_service(
    pg_repo: PostgresAnalyticsRepository = Depends(get_postgres_repository),
    redis_repo: RedisFeatureRepository = Depends(get_redis_repository),
    neo4j_repo: Neo4jGraphRepository = Depends(get_neo4j_repository),
) -> TopologyHealthService:
    return TopologyHealthService(
        postgres_repo=pg_repo,
        redis_repo=redis_repo,
        neo4j_repo=neo4j_repo,
        postgres_breaker=postgres_breaker,
        redis_breaker=redis_breaker,
        neo4j_breaker=neo4j_breaker,
    )
