from pydantic import BaseModel


class GraphOrderTrackingStatusModel(BaseModel):
    order_id: str
    product_id: str = ""
    origin_node_id: str = ""
    pickup_point_id: str = ""
    
    current_logistic_stage: int = 1
    calculated_delay_minutes: float = 0.0
    is_bottleneck_flag: int = 0
    failing_node_id: str = ""
    bottleneck_risk_factor: float = 0.0


class GraphNodeHealthModel(BaseModel):
    node_id: str
    status: str = "ACTIVE"
    is_bottleneck_flag: int = 0
    bottleneck_risk_factor: float = 0.0
    affected_personnel_count: int = 0
