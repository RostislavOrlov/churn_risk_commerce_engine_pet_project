from pydantic import BaseModel, ConfigDict

class BaseMasterSchema(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        frozen=True
    )
