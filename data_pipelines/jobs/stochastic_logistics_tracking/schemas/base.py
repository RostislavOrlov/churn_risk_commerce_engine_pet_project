from pydantic import BaseModel, ConfigDict


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(
        slots=True,
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True
    )
