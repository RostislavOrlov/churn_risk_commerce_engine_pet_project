from pydantic import Field
from .base import StrictBaseModel

class ClickHouseScanEventRow(StrictBaseModel):
    event_id: str = Field(..., description="Уникальный глобальный идентификатор (GUID) физического события сканирования штрихкода")
    package_id: str = Field(..., description="Идентификатор конкретной SKU-коробки или термосумки маркетплейса")
    order_id: str = Field(..., description="Идентификатор B2C-заказа розничного покупателя")
    timestamp: int = Field(..., ge=0, description="Временная метка считывания штрихкода (Unix Epoch в секундах)")
    status: str = Field(..., description="Физический статус чекпоинта прохождения магистрали (WH_RECEIVE, HUB_SORT, PVZ_ARRIVE и т.д.)")
    darkstore_id: str = Field(..., description="Идентификатор логистического узла (склада/хаба/ПВЗ), на воротах которого сработал сканер")
    executor_id: str = Field(..., description="Идентификатор сотрудника смены, водителя фуры или курьера последней мили")
    executor_type: str = Field(..., description="Категориальный тип исполнителя: WORKER (рабочий склада), DRIVER (магистраль), COURIER (последняя миля)")
