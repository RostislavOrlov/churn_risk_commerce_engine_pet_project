from __future__ import annotations

import logging
from typing import List
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from ..core.circuit_breaker import HighAvailabilityCircuitBreaker
from ..core.exceptions import RealTimeCommerceException
from ..repositories.base import BasePostgresRepository, BaseRedisRepository
from ..schemas.response import (
    ActiveIncidentsListResponse,
    IncidentItemResponse,
    IncidentRerouteResponse,
)
from ..tasks.workers import dispatch_reroute_command_tms

logger = logging.getLogger("rtc_logistics_ops")


class LogisticsOperationsService:
    def __init__(
        self,
        postgres_repo: BasePostgresRepository,
        redis_repo: BaseRedisRepository,
        postgres_breaker: HighAvailabilityCircuitBreaker,
        redis_breaker: HighAvailabilityCircuitBreaker,
    ):
        self.pg_repo = postgres_repo
        self.redis_repo = redis_repo
        self.postgres_breaker = postgres_breaker
        self.redis_breaker = redis_breaker

    async def get_active_incidents(
        self,
        replica_session: AsyncSession,
        limit: int = 50,
        offset: int = 0,
    ) -> ActiveIncidentsListResponse:
        cache_key_suffix = f"offset_{offset}"

        # 1. Cache-Aside: сначала читаем из Redis (L2)
        if self.redis_breaker.allow_request():
            try:
                cached_items = await self.redis_repo.get_active_incidents_cached(
                    failing_node_id=cache_key_suffix, limit=limit
                )
                self.redis_breaker.record_success()
                if cached_items is not None:
                    items = [IncidentItemResponse(**item) for item in cached_items]
                    return ActiveIncidentsListResponse(
                        source="L2_REDIS_CACHE",
                        total_count=len(items),
                        incidents=items,
                    )
            except Exception as redis_err:
                self.redis_breaker.record_failure(redis_err)

        # 2. Cache Miss: идем в Postgres Replica
        if not self.postgres_breaker.allow_request():
            raise RealTimeCommerceException(
                "Postgres Replica isolated by circuit breaker", code="DATASTORE_ISOLATED", status_code=503
            )

        try:
            records = await self.pg_repo.fetch_active_incidents(
                session=replica_session,
                limit=limit,
                offset=offset,
            )
            self.postgres_breaker.record_success()

            items: List[IncidentItemResponse] = [
                IncidentItemResponse(
                    incident_id=str(r.incident_id),
                    order_id=r.order_id,
                    failing_node_id=r.failing_node_id,
                    logistic_stage_idx=int(r.logistic_stage_idx),
                    calculated_delay_minutes=float(r.calculated_delay_minutes),
                    incident_status=r.incident_status,
                    assigned_team=r.assigned_team,
                    created_at=r.created_at,
                )
                for r in records
            ]

            # 3. Прогреваем Redis на 10 секунд
            if self.redis_breaker.allow_request():
                try:
                    serialized = [item.model_dump(mode="json") for item in items]
                    await self.redis_repo.set_active_incidents_cached(
                        items=serialized,
                        failing_node_id=cache_key_suffix,
                        limit=limit,
                        ttl_sec=10,
                    )
                except Exception as cache_err:
                    logger.warning(f"[CACHE_WARM_WARN][INCIDENTS] {cache_err}")

            return ActiveIncidentsListResponse(
                source="POSTGRES_REPLICA_FALLBACK",
                total_count=len(items),
                incidents=items,
            )
        except RealTimeCommerceException:
            raise
        except Exception as exc:
            self.postgres_breaker.record_failure(exc)
            logger.error(f"[LOGISTICS_OPS][FETCH_INCIDENTS_FAIL] {exc}")
            raise RealTimeCommerceException("Failed to fetch active incidents", status_code=500)

    async def reroute_incident(
        self,
        incident_id: UUID,
        assigned_team: str,
        bypass_hub_id: str,
        master_session: AsyncSession,
    ) -> IncidentRerouteResponse:
        if not self.postgres_breaker.allow_request():
            raise RealTimeCommerceException(
                "Postgres Master isolated by circuit breaker", code="MASTER_ISOLATED", status_code=503
            )

        try:
            updated = await self.pg_repo.update_incident_to_rerouted(
                session=master_session,
                incident_id=incident_id,
                assigned_team=assigned_team,
            )
            self.postgres_breaker.record_success()
        except Exception as exc:
            self.postgres_breaker.record_failure(exc)
            logger.error(f"[LOGISTICS_OPS][REROUTE_FAIL] incident_id={incident_id}: {exc}")
            raise RealTimeCommerceException("Failed to reroute incident in Master DB", status_code=500)

        if not updated:
            raise RealTimeCommerceException(
                f"Incident {incident_id} not found or already closed",
                code="INCIDENT_NOT_FOUND",
                status_code=404,
            )

        # Инвалидируем кэш активных инцидентов в Redis
        if self.redis_breaker.allow_request():
            try:
                await self.redis_repo.invalidate_active_incidents()
            except Exception as inv_err:
                logger.warning(f"[CACHE_INVALIDATE_WARN][INCIDENTS] {inv_err}")

        # Асинхронная постановка задачи в Celery для отправки вебхука в TMS
        tms_task_id = "BROKER_UNAVAILABLE"
        try:
            async_res = dispatch_reroute_command_tms.delay(
                incident_id=str(updated.incident_id),
                order_id=updated.order_id,
                failing_node_id=updated.failing_node_id,
                bypass_hub_id=bypass_hub_id,
                assigned_team=assigned_team,
            )
            tms_task_id = str(async_res.id)
        except Exception as celery_err:
            logger.warning(f"[CELERY_DISPATCH_WARN] incident_id={incident_id}: {celery_err}")

        return IncidentRerouteResponse(
            incident_id=str(updated.incident_id),
            order_id=updated.order_id,
            previous_status="RAISED",
            new_status=updated.incident_status,
            assigned_team=assigned_team,
            tms_task_id=tms_task_id,
            message=f"Incident rerouted to bypass hub {bypass_hub_id}",
        )
