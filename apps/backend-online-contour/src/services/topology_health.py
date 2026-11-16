from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from ..core.circuit_breaker import HighAvailabilityCircuitBreaker
from ..core.exceptions import RealTimeCommerceException
from ..models.graph_entities import GraphNodeHealthModel
from ..models.marts import NodeDynamicCapacityModel
from ..repositories.base import BaseNeo4jRepository, BasePostgresRepository, BaseRedisRepository
from ..schemas.response import (
    BlockedNodeCapacityItem,
    NodeHealthPassportResponse,
    TopologyCapacitiesResponse,
)

logger = logging.getLogger("rtc_topology_health")


class TopologyHealthService:
    def __init__(
        self,
        postgres_repo: BasePostgresRepository,
        redis_repo: BaseRedisRepository,
        postgres_breaker: HighAvailabilityCircuitBreaker,
        redis_breaker: HighAvailabilityCircuitBreaker,
        neo4j_repo: Optional[BaseNeo4jRepository] = None,
        neo4j_breaker: Optional[HighAvailabilityCircuitBreaker] = None,
    ):
        self.pg_repo = postgres_repo
        self.redis_repo = redis_repo
        self.neo4j_repo = neo4j_repo
        self.postgres_breaker = postgres_breaker
        self.redis_breaker = redis_breaker
        self.neo4j_breaker = neo4j_breaker

    @staticmethod
    def _get_l1_engine():
        from ..api.dependencies import global_cache_engine
        return global_cache_engine

    def _compute_passport_metrics(
        self,
        capacity: NodeDynamicCapacityModel,
        graph_health: GraphNodeHealthModel,
    ) -> NodeHealthPassportResponse:
        base_cap = max(1, int(capacity.base_capacity_packages))
        current_cap = max(0, int(capacity.current_capacity_packages))

        degraded_packages = max(0, base_cap - current_cap)
        utilization_pct = round((degraded_packages / base_cap) * 100.0, 2)

        # За стандартное отклонение (1 sigma) принимаем 15% от паспортной пропускной способности хаба
        sigma = base_cap * 0.15
        z_score = round((degraded_packages / sigma) + (float(capacity.bottleneck_risk_factor) * 1.5), 2)

        if int(capacity.is_active_block) == 1 or z_score >= 3.0:
            health_grade = "CRITICAL_BLOCKED"
        elif z_score >= 1.5 or float(capacity.bottleneck_risk_factor) >= 0.5:
            health_grade = "DEGRADED_WARNING"
        else:
            health_grade = "HEALTHY"

        return NodeHealthPassportResponse(
            node_id=capacity.node_id,
            node_type=capacity.node_type,
            operational_status=graph_health.status,
            base_capacity_packages=int(capacity.base_capacity_packages),
            current_capacity_packages=int(capacity.current_capacity_packages),
            capacity_utilization_pct=utilization_pct,
            bottleneck_risk_factor=float(capacity.bottleneck_risk_factor),
            is_bottleneck_flag=max(int(capacity.is_active_block), int(graph_health.is_bottleneck_flag)),
            affected_personnel_count=int(graph_health.affected_personnel_count),
            performance_lag_zscore=z_score,
            health_grade=health_grade,
        )

    async def get_restricted_nodes_capacities(
        self,
        replica_session: AsyncSession,
        risk_threshold: float = 0.5,
    ) -> TopologyCapacitiesResponse:
        cache_engine = self._get_l1_engine()
        l1_key = f"topology:blocked_nodes:{risk_threshold}"

        # 1. L1 Cache (ОЗУ воркера)
        l1_hit = cache_engine.get_l1(l1_key)
        if l1_hit is not None:
            items = [BlockedNodeCapacityItem(**item) for item in l1_hit]
            return TopologyCapacitiesResponse(
                source="L1_MEMORY_CACHE",
                total_restricted_nodes=len(items),
                nodes=items,
            )

        # 2. L2 Cache (Redis)
        if self.redis_breaker.allow_request():
            try:
                l2_nodes = await self.redis_repo.get_blocked_nodes_cached()
                self.redis_breaker.record_success()
                if l2_nodes is not None:
                    items = [BlockedNodeCapacityItem(**n) for n in l2_nodes]
                    serialized = [item.model_dump(mode="json") for item in items]
                    cache_engine.set_l1(l1_key, serialized)
                    return TopologyCapacitiesResponse(
                        source="L2_REDIS_CACHE",
                        total_restricted_nodes=len(items),
                        nodes=items,
                    )
            except Exception as redis_err:
                self.redis_breaker.record_failure(redis_err)

        # 3. Fallback: Postgres Replica
        if not self.postgres_breaker.allow_request():
            raise RealTimeCommerceException(
                "Postgres Replica isolated by circuit breaker", code="DATASTORE_ISOLATED", status_code=503
            )

        try:
            records = await self.pg_repo.fetch_blocked_or_risky_nodes(
                session=replica_session,
                risk_threshold=risk_threshold,
            )
            self.postgres_breaker.record_success()

            items: List[BlockedNodeCapacityItem] = [
                BlockedNodeCapacityItem(
                    node_id=r.node_id,
                    node_type=r.node_type,
                    base_capacity_packages=int(r.base_capacity_packages),
                    current_capacity_packages=int(r.current_capacity_packages),
                    bottleneck_risk_factor=float(r.bottleneck_risk_factor),
                    is_active_block=int(r.is_active_block),
                )
                for r in records
            ]

            serialized = [item.model_dump(mode="json") for item in items]
            cache_engine.set_l1(l1_key, serialized)

            if self.redis_breaker.allow_request():
                try:
                    await self.redis_repo.set_blocked_nodes_cached(serialized, ttl_sec=30)
                except Exception as cache_err:
                    logger.warning(f"[CACHE_WARM_WARN][TOPOLOGY] {cache_err}")

            return TopologyCapacitiesResponse(
                source="POSTGRES_REPLICA_FALLBACK",
                total_restricted_nodes=len(items),
                nodes=items,
            )
        except RealTimeCommerceException:
            raise
        except Exception as exc:
            self.postgres_breaker.record_failure(exc)
            logger.error(f"[TOPOLOGY_HEALTH][FETCH_NODES_FAIL] {exc}")
            raise RealTimeCommerceException("Failed to fetch blocked node capacities", status_code=500)

    async def get_node_health_passport(
        self,
        node_id: str,
        replica_session: AsyncSession,
    ) -> NodeHealthPassportResponse:
        async def _fetch_capacity_safe() -> Optional[NodeDynamicCapacityModel]:
            if not self.postgres_breaker.allow_request():
                raise RealTimeCommerceException(
                    "Postgres Replica isolated by circuit breaker", code="DATASTORE_ISOLATED", status_code=503
                )
            try:
                rec = await self.pg_repo.fetch_node_capacity_by_id(replica_session, node_id)
                self.postgres_breaker.record_success()
                return rec
            except Exception as exc:
                self.postgres_breaker.record_failure(exc)
                raise RealTimeCommerceException(f"Failed to fetch node {node_id}", status_code=500)

        async def _fetch_graph_safe() -> GraphNodeHealthModel:
            if not self.neo4j_repo or (self.neo4j_breaker and not self.neo4j_breaker.allow_request()):
                return GraphNodeHealthModel(node_id=node_id)
            try:
                res = await self.neo4j_repo.fetch_node_health_metrics(node_id)
                if self.neo4j_breaker:
                    self.neo4j_breaker.record_success()
                return res
            except Exception as exc:
                if self.neo4j_breaker:
                    self.neo4j_breaker.record_failure(exc)
                logger.warning(f"[TOPOLOGY_HEALTH][NEO4J_DEGRADED] node_id={node_id}: {exc}")
                return GraphNodeHealthModel(node_id=node_id)

        capacity_record, graph_health = await asyncio.gather(
            _fetch_capacity_safe(),
            _fetch_graph_safe(),
        )

        if not capacity_record:
            raise RealTimeCommerceException(
                f"Logistics node '{node_id}' not found", code="NODE_NOT_FOUND", status_code=404
            )

        return self._compute_passport_metrics(capacity_record, graph_health)
