from pydantic import BaseModel, Field


class ChurnInferenceRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=64, examples=["USR_0000042"])
    order_id: str = Field(..., min_length=3, max_length=64, examples=["ORD_0000042"])


class IncidentRerouteRequest(BaseModel):
    assigned_team: str = Field(default="DISPATCH_NIGHT_SHIFT", min_length=2, max_length=64)
    bypass_hub_id: str = Field(default="HUB_005", min_length=2, max_length=64)
    reason: str = Field(default="Leiden bottleneck threshold exceeded", max_length=256)
