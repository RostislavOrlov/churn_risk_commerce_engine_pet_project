from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class ChurnInferenceResponse(BaseModel):
    user_id: str
    order_id: str
    churn_probability: float
    is_high_risk: bool
    degraded_mode: bool = False
    action_taken: str
    voucher_code: Optional[str] = None
    discount_amount_rub: float = 0.0
    inference_latency_ms: float


class CompensationItemResponse(BaseModel):
    compensation_id: int
    order_id: str
    churn_risk_score: float
    voucher_code: str
    discount_amount_rub: float
    is_sent_flag: int
    sent_at: Optional[datetime] = None
    expires_at: datetime


class CustomerCompensationsListResponse(BaseModel):
    user_id: str
    total_count: int
    items: List[CompensationItemResponse]


class IncidentItemResponse(BaseModel):
    incident_id: Optional[str] = None
    order_id: str
    failing_node_id: str
    logistic_stage_idx: int
    calculated_delay_minutes: float
    incident_status: str
    assigned_team: str
    created_at: Optional[datetime] = None


class ActiveIncidentsListResponse(BaseModel):
    source: str
    total_count: int
    incidents: List[IncidentItemResponse]


class IncidentRerouteResponse(BaseModel):
    incident_id: str
    order_id: str
    previous_status: str = "RAISED"
    new_status: str = "REROUTED"
    assigned_team: str
    tms_task_id: str
    message: str


class BlockedNodeCapacityItem(BaseModel):
    node_id: str
    node_type: str
    base_capacity_packages: int
    current_capacity_packages: int
    bottleneck_risk_factor: float
    is_active_block: int


class TopologyCapacitiesResponse(BaseModel):
    source: str
    total_restricted_nodes: int
    nodes: List[BlockedNodeCapacityItem]


class NodeHealthPassportResponse(BaseModel):
    node_id: str
    node_type: str
    operational_status: str
    base_capacity_packages: int
    current_capacity_packages: int
    capacity_utilization_pct: float
    bottleneck_risk_factor: float
    is_bottleneck_flag: int
    affected_personnel_count: int
    performance_lag_zscore: float = Field(..., description="Z-Score отклонения пропускной способности и риска")
    health_grade: str
