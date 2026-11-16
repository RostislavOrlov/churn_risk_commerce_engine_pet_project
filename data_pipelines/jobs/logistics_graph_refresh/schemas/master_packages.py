from pydantic import Field, field_validator
from data_pipelines.jobs.logistics_graph_refresh.schemas.base import BaseMasterSchema

class MasterPackageSchema(BaseMasterSchema):
    package_id: str = Field(..., min_length=1)
    order_id: str = Field(..., min_length=1)
    package_type: str = Field(default="BOX")
    weight: float = Field(default=0.0)

    @field_validator("weight")
    @classmethod
    def validate_weight_bounds(cls, value: float) -> float:
        if not (0.0 <= value <= 500.0):
            return 0.0
        return value
