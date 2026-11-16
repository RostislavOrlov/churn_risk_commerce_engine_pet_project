from __future__ import annotations

import asyncio
import secrets
import logging
from decimal import Decimal
from typing import Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..core.circuit_breaker import HighAvailabilityCircuitBreaker, GracefulDegradationEngine
from ..core.exceptions import RealTimeCommerceException
from ..models.cache_entities import UserOnlineFeaturesModel
from ..models.graph_entities import GraphOrderTrackingStatusModel
from ..repositories.base import BasePostgresRepository, BaseRedisRepository, BaseNeo4jRepository
from ..schemas.response import (
    ChurnInferenceResponse,
    CustomerCompensationsListResponse,
    CompensationItemResponse,
)
from .analytics import ChurnModelServingService

logger = logging.getLogger("rtc_action_engine")


class ActionEngineService:
    def __init__(
        self,
        postgres_repo: BasePostgresRepository,
        redis_repo: BaseRedisRepository,
        neo4j_repo: BaseNeo4jRepository,
        ml_service: ChurnModelServingService,
        redis_breaker: HighAvailabilityCircuitBreaker,
        neo4j_breaker: HighAvailabilityCircuitBreaker,
        postgres_breaker: HighAvailabilityCircuitBreaker,
    ):
        self.pg_repo = postgres_repo
        self.redis_repo = redis_repo
        self.neo4j_repo = neo4j_repo
        self.ml_service = ml_service
        self.redis_breaker = redis_breaker
        self.neo4j_breaker = neo4j_breaker
        self.postgres_breaker = postgres_breaker

    def _calculate_compensation_rub(
        self,
        churn_prob: float,
        user_ltv: float,
        customer_segment: str,
        margin_info: Optional[dict | object],
        delay_minutes: float,
    ) -> Decimal:
        is_vip = user_ltv >= settings.VIP_LTV_THRESHOLD_RUB or customer_segment == "VIP_RETENTION"

        if isinstance(margin_info, dict):
            is_express_grocery = int(margin_info.get("is_express_grocery", 0)) == 1
            order_margin = float(margin_info.get("order_margin_rub", 0.0))
        elif margin_info is not None:
            is_express_grocery = int(getattr(margin_info, "is_express_grocery", 0)) == 1
            order_margin = float(getattr(margin_info, "order_margin_rub", 0.0))
        else:
            is_express_grocery = False
            order_margin = 0.0

        if is_vip:
            base_discount = 400.0 if churn_prob >= 0.85 else 300.0
        elif is_express_grocery and delay_minutes > settings.EGROCERY_MAX_DELAY_MINUTES:
            base_discount = 250.0
        else:
            base_discount = 150.0

        if not is_vip and order_margin > 0:
            max_by_margin = order_margin * 0.8
            base_discount = min(base_discount, max(100.0, max_by_margin))

        final_amount = min(base_discount, settings.MAX_VOUCHER_COMPENSATION_RUB)
        return Decimal(f"{final_amount:.2f}")

    async def _fallback_tracking_from_redis(
        self, order_id: str, user_features: Optional[UserOnlineFeaturesModel]
    ) -> GraphOrderTrackingStatusModel:
        if not self.redis_breaker.allow_request():
            return GraphOrderTrackingStatusModel(order_id=order_id)

        try:
            if user_features and user_features.origin_node_id:
                node_cap = await self.redis_repo.get_node_capacity_cached(user_features.origin_node_id)
                if node_cap and (node_cap.is_active_block == 1 or node_cap.bottleneck_risk_factor > 0.5):
                    return GraphOrderTrackingStatusModel(
                        order_id=order_id,
                        current_logistic_stage=2,
                        calculated_delay_minutes=round(node_cap.bottleneck_risk_factor * 40.0, 2),
                        is_bottleneck_flag=1,
                        failing_node_id=node_cap.node_id,
                        bottleneck_risk_factor=node_cap.bottleneck_risk_factor,
                    )

            blocked_nodes = await self.redis_repo.get_blocked_nodes_cached()
            if blocked_nodes:
                worst = blocked_nodes[0]
                risk = float(worst.get("bottleneck_risk_factor", 0.6))
                return GraphOrderTrackingStatusModel(
                    order_id=order_id,
                    current_logistic_stage=3,
                    calculated_delay_minutes=round(risk * 35.0, 2),
                    is_bottleneck_flag=1,
                    failing_node_id=str(worst.get("node_id", "")),
                    bottleneck_risk_factor=risk,
                )
        except Exception as err:
            logger.warning(f"[DEGRADATION][REDIS_NODE_FALLBACK_FAIL] {err}")

        return GraphOrderTrackingStatusModel(order_id=order_id)

    async def _get_ltv_and_margin_cache_aside(
        self,
        user_id: str,
        order_id: str,
        replica_session: AsyncSession,
    ) -> Tuple[Optional[dict], Optional[dict], bool]:
        ltv_data: Optional[dict] = None
        margin_data: Optional[dict] = None
        db_degraded = False

        # 1. Cache-Aside: сначала идем в Redis (L2)
        if self.redis_breaker.allow_request():
            try:
                ltv_data, margin_data = await asyncio.gather(
                    self.redis_repo.get_user_ltv_cached(user_id),
                    self.redis_repo.get_market_margin_cached(order_id),
                )
                self.redis_breaker.record_success()
            except Exception as redis_err:
                self.redis_breaker.record_failure(redis_err)

        # 2. Cache Miss: идем в Postgres Replica и прогреваем Redis
        if (ltv_data is None or margin_data is None) and self.postgres_breaker.allow_request():
            try:
                if ltv_data is None:
                    ltv_row = await self.pg_repo.fetch_user_ltv(replica_session, user_id)
                    if ltv_row:
                        ltv_data = {
                            "long_term_value_rub": float(ltv_row.long_term_value_rub),
                            "customer_segment": ltv_row.customer_segment,
                        }
                        if self.redis_breaker.allow_request():
                            await self.redis_repo.set_user_ltv_cached(user_id, ltv_data)

                if margin_data is None:
                    margin_row = await self.pg_repo.fetch_market_margin(replica_session, order_id)
                    if margin_row:
                        margin_data = {
                            "order_margin_rub": float(margin_row.order_margin_rub),
                            "is_express_grocery": int(margin_row.is_express_grocery),
                        }
                        if self.redis_breaker.allow_request():
                            await self.redis_repo.set_market_margin_cached(order_id, margin_data)

                self.postgres_breaker.record_success()
            except Exception as pg_err:
                self.postgres_breaker.record_failure(pg_err)
                db_degraded = True

        return ltv_data, margin_data, db_degraded

    async def run_churn_inference_pipeline(
        self,
        user_id: str,
        order_id: str,
        master_session: AsyncSession,
        replica_session: AsyncSession,
    ) -> ChurnInferenceResponse:
        redis_failed = False
        neo4j_failed = False

        async def _fetch_redis_safe() -> Optional[UserOnlineFeaturesModel]:
            nonlocal redis_failed
            if not self.redis_breaker.allow_request():
                redis_failed = True
                return None
            try:
                res = await self.redis_repo.get_user_online_features(user_id)
                self.redis_breaker.record_success()
                return res
            except Exception as exc:
                self.redis_breaker.record_failure(exc)
                redis_failed = True
                return None

        async def _fetch_neo4j_safe() -> Tuple[Optional[GraphOrderTrackingStatusModel], bool]:
            nonlocal neo4j_failed
            if not self.neo4j_breaker.allow_request():
                neo4j_failed = True
                return None, True
            try:
                res = await self.neo4j_repo.fetch_order_live_tracking(order_id)
                self.neo4j_breaker.record_success()
                return res, False
            except Exception as exc:
                self.neo4j_breaker.record_failure(exc)
                neo4j_failed = True
                return None, True

        user_features, (live_tracking, graph_unavailable) = await asyncio.gather(
            _fetch_redis_safe(), _fetch_neo4j_safe()
        )

        if graph_unavailable or live_tracking is None:
            live_tracking = await self._fallback_tracking_from_redis(order_id, user_features)

        if user_features is None:
            redis_failed = True
            user_features = GracefulDegradationEngine.get_fallback_user_features(user_id)

        degraded_mode = redis_failed or neo4j_failed

        churn_prob, latency_ms = await self.ml_service.predict_churn_probability(
            user_features=user_features,
            live_tracking=live_tracking,
            degraded_mode=degraded_mode,
        )

        ltv_data, margin_data, pg_degraded = await self._get_ltv_and_margin_cache_aside(
            user_id=user_id,
            order_id=order_id,
            replica_session=replica_session,
        )
        if pg_degraded:
            degraded_mode = True

        effective_ltv = float(ltv_data["long_term_value_rub"]) if ltv_data else user_features.user_ltv
        effective_segment = str(ltv_data["customer_segment"]) if ltv_data else user_features.customer_segment
        delay_minutes = live_tracking.calculated_delay_minutes

        is_vip_degraded_protection = bool(
            redis_failed
            and (effective_ltv >= settings.VIP_LTV_THRESHOLD_RUB or effective_segment == "VIP_RETENTION")
            and delay_minutes > settings.EGROCERY_MAX_DELAY_MINUTES
        )
        is_express_sla_breach = bool(
            margin_data
            and margin_data.get("is_express_grocery") == 1
            and delay_minutes > settings.EGROCERY_MAX_DELAY_MINUTES
        )
        is_high_risk = (
            churn_prob >= settings.CHURN_RISK_THRESHOLD
            or is_express_sla_breach
            or is_vip_degraded_protection
        )

        if not is_high_risk:
            return ChurnInferenceResponse(
                user_id=user_id,
                order_id=order_id,
                churn_probability=churn_prob,
                is_high_risk=False,
                degraded_mode=degraded_mode,
                action_taken="NO_ACTION_REQUIRED",
                inference_latency_ms=latency_ms,
            )

        discount_rub = self._calculate_compensation_rub(
            churn_prob=churn_prob,
            user_ltv=effective_ltv,
            customer_segment=effective_segment,
            margin_info=margin_data,
            delay_minutes=delay_minutes,
        )
        voucher_code = f"CARE-{secrets.token_hex(4).upper()}"

        try:
            comp_record = await self.pg_repo.insert_preemptive_compensation(
                session=master_session,
                user_id=user_id,
                order_id=order_id,
                churn_risk_score=churn_prob,
                voucher_code=voucher_code,
                discount_amount_rub=discount_rub,
            )
            self.postgres_breaker.record_success()

            if self.redis_breaker.allow_request():
                try:
                    await self.redis_repo.invalidate_user_compensations(user_id)
                except Exception as inv_err:
                    logger.warning(f"[CACHE_INVALIDATE_WARN] user_id={user_id}: {inv_err}")

            return ChurnInferenceResponse(
                user_id=user_id,
                order_id=order_id,
                churn_probability=churn_prob,
                is_high_risk=True,
                degraded_mode=degraded_mode,
                action_taken="PREEMPTIVE_VOUCHER_ISSUED",
                voucher_code=comp_record.voucher_code,
                discount_amount_rub=float(comp_record.discount_amount_rub),
                inference_latency_ms=latency_ms,
            )
        except Exception as db_err:
            self.postgres_breaker.record_failure(db_err)
            logger.error(f"[ACTION_ENGINE][MASTER_WRITE_FAIL] order_id={order_id}: {db_err}")
            return ChurnInferenceResponse(
                user_id=user_id,
                order_id=order_id,
                churn_probability=churn_prob,
                is_high_risk=True,
                degraded_mode=True,
                action_taken="COMPENSATION_PERSIST_DEGRADED",
                inference_latency_ms=latency_ms,
            )

    async def get_customer_compensations(
        self,
        user_id: str,
        replica_session: AsyncSession,
    ) -> CustomerCompensationsListResponse:
        # 1. Cache-Aside: сначала идем в Redis (L2)
        if self.redis_breaker.allow_request():
            try:
                cached_items = await self.redis_repo.get_user_compensations_cached(user_id)
                self.redis_breaker.record_success()
                if cached_items is not None:
                    items = [CompensationItemResponse(**item) for item in cached_items]
                    return CustomerCompensationsListResponse(user_id=user_id, total_count=len(items), items=items)
            except Exception as redis_err:
                self.redis_breaker.record_failure(redis_err)

        # 2. Cache Miss: идем в Postgres Replica
        if not self.postgres_breaker.allow_request():
            raise RealTimeCommerceException(
                "Postgres Replica is isolated by circuit breaker", code="DATASTORE_ISOLATED", status_code=503
            )
        try:
            records = await self.pg_repo.fetch_user_compensations(replica_session, user_id)
            self.postgres_breaker.record_success()
            items = [
                CompensationItemResponse(
                    compensation_id=r.compensation_id,
                    order_id=r.order_id,
                    churn_risk_score=float(r.churn_risk_score),
                    voucher_code=r.voucher_code,
                    discount_amount_rub=float(r.discount_amount_rub),
                    is_sent_flag=int(r.is_sent_flag),
                    sent_at=r.sent_at,
                    expires_at=r.expires_at,
                )
                for r in records
            ]

            # 3. Прогреваем Redis на 60 секунд
            if self.redis_breaker.allow_request():
                try:
                    serialized = [item.model_dump(mode="json") for item in items]
                    await self.redis_repo.set_user_compensations_cached(user_id, serialized, ttl_sec=60)
                except Exception as cache_err:
                    logger.warning(f"[CACHE_WARM_WARN] compensations user_id={user_id}: {cache_err}")

            return CustomerCompensationsListResponse(user_id=user_id, total_count=len(items), items=items)
        except Exception as exc:
            self.postgres_breaker.record_failure(exc)
            raise RealTimeCommerceException("Failed to fetch customer compensations", status_code=500)
