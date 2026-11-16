import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import String, Integer, SmallInteger, Float, Numeric, DateTime, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class OrderModel(Base):
    __tablename__ = "orders"
    __table_args__ = {"schema": "public"}

    order_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="CREATED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))


class DwhUserLtvModel(Base):
    __tablename__ = "dwh_user_ltv"
    __table_args__ = {"schema": "dw_marts"}

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    long_term_value_rub: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    customer_segment: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))


class DwhMarketMarginModel(Base):
    __tablename__ = "dwh_market_margins"
    __table_args__ = {"schema": "dw_marts"}

    order_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    order_margin_rub: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    is_express_grocery: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))


class IncidentLogModel(Base):
    __tablename__ = "incident_log"
    __table_args__ = {"schema": "dw_marts"}

    incident_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    order_id: Mapped[str] = mapped_column(String(64), nullable=False)
    failing_node_id: Mapped[str] = mapped_column(String(64), nullable=False)
    logistic_stage_idx: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    calculated_delay_minutes: Mapped[float] = mapped_column(Float(precision=24), nullable=False)
    incident_status: Mapped[str] = mapped_column(String(32), nullable=False)
    assigned_team: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))


class PreemptiveCompensationModel(Base):
    __tablename__ = "preemptive_compensations"
    __table_args__ = {"schema": "dw_marts"}

    compensation_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    order_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    churn_risk_score: Mapped[float] = mapped_column(Float(precision=24), nullable=False)
    voucher_code: Mapped[str] = mapped_column(String(32), nullable=False)
    discount_amount_rub: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    is_sent_flag: Mapped[int] = mapped_column(SmallInteger, default=0)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class NodeDynamicCapacityModel(Base):
    __tablename__ = "node_dynamic_capacities"
    __table_args__ = {"schema": "dw_marts"}

    node_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    node_type: Mapped[str] = mapped_column(String(32), nullable=False)
    base_capacity_packages: Mapped[int] = mapped_column(Integer, nullable=False)
    current_capacity_packages: Mapped[int] = mapped_column(Integer, nullable=False)
    bottleneck_risk_factor: Mapped[float] = mapped_column(Float(precision=24), default=0.0)
    is_active_block: Mapped[int] = mapped_column(SmallInteger, default=0)
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW()"))
