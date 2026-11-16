from pydantic import Field, field_validator
from data_pipelines.jobs.logistics_graph_refresh.schemas.base import BaseMasterSchema

class MasterOrderSchema(BaseMasterSchema):
    user_id: str = Field(..., min_length=1)
    order_id: str = Field(..., min_length=1)
    amount: float = Field(default=0.0)
    status: str = Field(default="CREATED")

    @field_validator("amount")
    @classmethod
    def validate_amount_bounds(cls, value: float) -> float:
        if value < 0.0:
            return 0.0
        return value
