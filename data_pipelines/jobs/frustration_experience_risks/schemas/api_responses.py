from pydantic import Field
from datetime import datetime
from .base import StrictBaseModel
from .output_targets import CustomerFrustrationPassport


class SupportDashboardResponse(StrictBaseModel):
    status: str = Field("success", description="Флаг успешности обработки запроса к Redis L2 кэшу")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Системное время формирования ответа на ноде API шлюза")
    passport: CustomerFrustrationPassport = Field(..., description="Валидированный паспорт фрустрации клиента, поднятый из Redis по хэш-маске ключа")


class LogisticsRiskAlert(StrictBaseModel):
    warehouse_id: str = Field(..., description="Идентификатор склада инфраструктуры поставок ритейла")
    pickup_point_id: str = Field(..., description="Идентификатор целевого пункта выдачи заказов (ПВЗ) цепи")
    bottleneck_risk_factor: float = Field(..., ge=0.0, description="Критический фактор инфраструктурной перегрузки логистического коридора")
    affected_users_count: int = Field(..., ge=0, description="Количество уникальных пользователей, попавших в зону сбоя коридора за текущие сутки")
    estimated_lost_margin: float = Field(..., ge=0.0, description="Суммарная финансовая оценка упущенной маржи (LTV-at-Risk) по данному направлению в рублях")
