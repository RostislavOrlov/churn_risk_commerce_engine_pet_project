from pydantic import Field
from .base import StrictBaseModel

class OrderStochasticDelayPassport(StrictBaseModel):
    order_id: str = Field(..., description="Уникальный глобальный идентификатор B2C-заказа")
    calculated_delay_minutes: float = Field(..., ge=0.0, description="Расчитанное в GDS среднее время стохастического опоздания курьера/магистрали в минутах")
    current_logistic_stage: int = Field(..., ge=1, le=8, description="Текущий зафиксированный физический чекпоинт прохождения магистрали из 8 доступных")
    is_bottleneck_flag: int = Field(..., ge=0, le=1, description="Бинарный флаг нахождения посылки внутри логистического тромба, определенного алгоритмом Leiden (0 или 1)")
    failing_node_id: str = Field(..., description="Идентификатор сбойного хаба, кросс-дока или даркстора, ставшего первопричиной затора")
