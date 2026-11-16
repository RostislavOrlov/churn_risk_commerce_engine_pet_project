import uuid
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import select, update, or_, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..config import settings
from ..models.marts import (
    DwhUserLtvModel,
    DwhMarketMarginModel,
    IncidentLogModel,
    PreemptiveCompensationModel,
    NodeDynamicCapacityModel,
)
from .base import BasePostgresRepository

logger = logging.getLogger("rtc_postgres_repo")


class PostgresDatabaseManager:
    """Управляет двумя независимыми пулами SQLAlchemy: Master (Write) и Replica (Read)."""

    def __init__(self) -> None:
        self.master_engine: Optional[AsyncEngine] = None
        self.replica_engine: Optional[AsyncEngine] = None
        self.master_session_factory: Optional[async_sessionmaker[AsyncSession]] = None
        self.replica_session_factory: Optional[async_sessionmaker[AsyncSession]] = None

    def init_engines(self) -> None:
        self.master_engine = create_async_engine(
            settings.POSTGRES_MASTER_DSN,
            pool_size=settings.POSTGRES_MIN_POOL_SIZE,
            max_overflow=max(1, settings.POSTGRES_MAX_POOL_SIZE - settings.POSTGRES_MIN_POOL_SIZE),
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={"command_timeout": settings.POSTGRES_TIMEOUT_SEC},
        )
        self.replica_engine = create_async_engine(
            settings.POSTGRES_REPLICA_DSN,
            pool_size=settings.POSTGRES_MIN_POOL_SIZE,
            max_overflow=max(1, settings.POSTGRES_MAX_POOL_SIZE - settings.POSTGRES_MIN_POOL_SIZE),
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={"command_timeout": settings.POSTGRES_TIMEOUT_SEC},
        )
        self.master_session_factory = async_sessionmaker(
            bind=self.master_engine, class_=AsyncSession, expire_on_commit=False
        )
        self.replica_session_factory = async_sessionmaker(
            bind=self.replica_engine, class_=AsyncSession, expire_on_commit=False
        )
        logger.info("[POSTGRES] Master & Replica async engines initialized.")

    async def dispose(self) -> None:
        if self.master_engine:
            await self.master_engine.dispose()
        if self.replica_engine:
            await self.replica_engine.dispose()
        logger.info("[POSTGRES] Master & Replica async engines disposed.")


pg_manager = PostgresDatabaseManager()


class PostgresAnalyticsRepository(BasePostgresRepository):
    async def fetch_user_ltv(self, session: AsyncSession, user_id: str) -> Optional[DwhUserLtvModel]:
        stmt = select(DwhUserLtvModel).where(DwhUserLtvModel.user_id == user_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def fetch_market_margin(self, session: AsyncSession, order_id: str) -> Optional[DwhMarketMarginModel]:
        stmt = select(DwhMarketMarginModel).where(DwhMarketMarginModel.order_id == order_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def fetch_user_compensations(
        self, session: AsyncSession, user_id: str
    ) -> List[PreemptiveCompensationModel]:
        stmt = (
            select(PreemptiveCompensationModel)
            .where(PreemptiveCompensationModel.user_id == user_id)
            .order_by(desc(PreemptiveCompensationModel.compensation_id))
            .limit(50)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def fetch_compensation_by_order(
        self, session: AsyncSession, order_id: str
    ) -> Optional[PreemptiveCompensationModel]:
        stmt = select(PreemptiveCompensationModel).where(PreemptiveCompensationModel.order_id == order_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def insert_preemptive_compensation(
        self,
        session: AsyncSession,
        user_id: str,
        order_id: str,
        churn_risk_score: float,
        voucher_code: str,
        discount_amount_rub: Decimal,
    ) -> PreemptiveCompensationModel:
        expires_at = datetime.now(timezone.utc) + timedelta(days=7)

        insert_stmt = (
            pg_insert(PreemptiveCompensationModel)
            .values(
                user_id=user_id,
                order_id=order_id,
                churn_risk_score=churn_risk_score,
                voucher_code=voucher_code,
                discount_amount_rub=discount_amount_rub,
                is_sent_flag=0,
                expires_at=expires_at,
            )
            .on_conflict_do_nothing(index_elements=["order_id"])
            .returning(PreemptiveCompensationModel)
        )
        result = await session.execute(insert_stmt)
        created = result.scalar_one_or_none()
        if created is not None:
            return created

        existing = await self.fetch_compensation_by_order(session, order_id)
        if existing is not None:
            return existing
        raise RuntimeError(f"Failed to upsert compensation for order_id={order_id}")

    async def fetch_active_incidents(
        self, session: AsyncSession, limit: int = 50, offset: int = 0
    ) -> List[IncidentLogModel]:
        stmt = (
            select(IncidentLogModel)
            .where(IncidentLogModel.incident_status.in_(["RAISED", "REROUTED"]))
            .order_by(desc(IncidentLogModel.calculated_delay_minutes))
            .limit(limit)
            .offset(offset)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def update_incident_to_rerouted(
        self, session: AsyncSession, incident_id: uuid.UUID, assigned_team: str
    ) -> Optional[IncidentLogModel]:
        stmt = (
            update(IncidentLogModel)
            .where(IncidentLogModel.incident_id == incident_id)
            .values(
                incident_status="REROUTED",
                assigned_team=assigned_team,
                updated_at=datetime.now(timezone.utc),
            )
            .returning(IncidentLogModel)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def fetch_blocked_or_risky_nodes(
        self, session: AsyncSession, risk_threshold: float = 0.5
    ) -> List[NodeDynamicCapacityModel]:
        stmt = (
            select(NodeDynamicCapacityModel)
            .where(
                or_(
                    NodeDynamicCapacityModel.is_active_block == 1,
                    NodeDynamicCapacityModel.bottleneck_risk_factor > risk_threshold,
                )
            )
            .order_by(desc(NodeDynamicCapacityModel.bottleneck_risk_factor))
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def fetch_node_capacity_by_id(
        self, session: AsyncSession, node_id: str
    ) -> Optional[NodeDynamicCapacityModel]:
        stmt = select(NodeDynamicCapacityModel).where(NodeDynamicCapacityModel.node_id == node_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
