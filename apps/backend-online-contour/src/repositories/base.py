from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from ..models.cache_entities import NodeDynamicCapacityCacheModel, UserOnlineFeaturesModel
from ..models.graph_entities import GraphNodeHealthModel, GraphOrderTrackingStatusModel
from ..models.marts import (
    DwhMarketMarginModel,
    DwhUserLtvModel,
    IncidentLogModel,
    NodeDynamicCapacityModel,
    PreemptiveCompensationModel,
)


class BasePostgresRepository(ABC):
    @abstractmethod
    async def fetch_user_ltv(self, session: AsyncSession, user_id: str) -> Optional[DwhUserLtvModel]:
        ...

    @abstractmethod
    async def fetch_market_margin(self, session: AsyncSession, order_id: str) -> Optional[DwhMarketMarginModel]:
        ...

    @abstractmethod
    async def insert_preemptive_compensation(
        self,
        session: AsyncSession,
        user_id: str,
        order_id: str,
        churn_risk_score: float,
        voucher_code: str,
        discount_amount_rub: Decimal,
    ) -> PreemptiveCompensationModel:
        ...

    @abstractmethod
    async def fetch_user_compensations(
        self, session: AsyncSession, user_id: str
    ) -> List[PreemptiveCompensationModel]:
        ...

    @abstractmethod
    async def fetch_active_incidents(
        self, session: AsyncSession, limit: int = 50, offset: int = 0
    ) -> List[IncidentLogModel]:
        ...

    @abstractmethod
    async def update_incident_to_rerouted(
        self, session: AsyncSession, incident_id: UUID, assigned_team: str
    ) -> Optional[IncidentLogModel]:
        ...

    @abstractmethod
    async def fetch_blocked_or_risky_nodes(
        self, session: AsyncSession, risk_threshold: float = 0.5
    ) -> List[NodeDynamicCapacityModel]:
        ...

    @abstractmethod
    async def fetch_node_capacity_by_id(
        self, session: AsyncSession, node_id: str
    ) -> Optional[NodeDynamicCapacityModel]:
        ...


class BaseRedisRepository(ABC):
    @abstractmethod
    async def ping(self) -> bool:
        ...

    @abstractmethod
    async def get_user_online_features(self, user_id: str) -> Optional[UserOnlineFeaturesModel]:
        ...

    @abstractmethod
    async def get_user_ltv_cached(self, user_id: str) -> Optional[Dict[str, Any]]:
        ...

    @abstractmethod
    async def set_user_ltv_cached(self, user_id: str, payload: Dict[str, Any], ttl_sec: int = 3600) -> None:
        ...

    @abstractmethod
    async def get_market_margin_cached(self, order_id: str) -> Optional[Dict[str, Any]]:
        ...

    @abstractmethod
    async def set_market_margin_cached(self, order_id: str, payload: Dict[str, Any], ttl_sec: int = 1800) -> None:
        ...

    @abstractmethod
    async def get_user_compensations_cached(self, user_id: str) -> Optional[List[Dict[str, Any]]]:
        ...

    @abstractmethod
    async def set_user_compensations_cached(
        self, user_id: str, items: List[Dict[str, Any]], ttl_sec: int = 60
    ) -> None:
        ...

    @abstractmethod
    async def invalidate_user_compensations(self, user_id: str) -> None:
        ...

    @abstractmethod
    async def get_active_incidents_cached(
        self, failing_node_id: Optional[str] = None, limit: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        ...

    @abstractmethod
    async def set_active_incidents_cached(
        self,
        items: List[Dict[str, Any]],
        failing_node_id: Optional[str] = None,
        limit: int = 100,
        ttl_sec: int = 10,
    ) -> None:
        ...

    @abstractmethod
    async def invalidate_active_incidents(self) -> None:
        ...

    @abstractmethod
    async def get_node_capacity_cached(self, node_id: str) -> Optional[NodeDynamicCapacityCacheModel]:
        ...

    @abstractmethod
    async def set_node_capacity_cached(
        self, node_id: str, payload: Dict[str, Any], ttl_sec: int = 30
    ) -> None:
        ...

    @abstractmethod
    async def get_blocked_nodes_cached(self) -> Optional[List[Dict[str, Any]]]:
        ...

    @abstractmethod
    async def set_blocked_nodes_cached(self, nodes: List[Dict[str, Any]], ttl_sec: int = 30) -> None:
        ...


class BaseNeo4jRepository(ABC):
    @abstractmethod
    async def fetch_order_live_tracking(self, order_id: str) -> GraphOrderTrackingStatusModel:
        ...

    @abstractmethod
    async def fetch_node_health_metrics(self, node_id: str) -> GraphNodeHealthModel:
        ...
