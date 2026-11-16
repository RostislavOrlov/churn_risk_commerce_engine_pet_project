from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class UserOnlineFeaturesModel(BaseModel):
    user_id: str

    # Поля, загружаемые из Redis HASH online:features:user:{user_id} (из redis_sync.py)
    max_session_depth: int = 1
    p90_frustration: float = 0.0
    total_rage_clicks: int = 0
    pogo_sticking_count: int = 0
    max_velocity_volatility: float = 0.0
    max_unique_products: int = 1
    max_unique_categories: int = 1
    total_perf_events: int = 0
    api_errors_count: int = 0
    p95_interface_render_ms: float = 0.0
    avg_network_ms: float = 0.0
    total_searches: int = 0
    total_filter_resets: int = 0
    max_search_scroll: int = 0
    promo_price: float = 0.0
    has_bundles: int = 0
    category_depth: int = 1  

    user_ltv: float = 0.0
    customer_segment: str = "REGULAR"

    origin_node_id: str = ""
    pickup_point_id: str = ""
    route_time: float = 12.0
    route_cost: float = 0.0
    route_jitter: float = 0.0
    bottleneck_risk_factor: float = 0.0
    graph_hops: int = 1
    isolation_score: float = 0.0

    is_fallback: bool = Field(default=False)


class NodeDynamicCapacityCacheModel(BaseModel):
    node_id: str
    node_type: str = "TRANSIT_HUB"
    base_capacity_packages: int = 1000
    current_capacity_packages: int = 1000
    bottleneck_risk_factor: float = 0.0
    is_active_block: int = 0
    last_updated_at: Optional[datetime] = None
