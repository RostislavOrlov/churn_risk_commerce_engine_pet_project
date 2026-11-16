from pydantic import Field
from .base import StrictBaseModel


class GreenplumStagingRow(StrictBaseModel):
    user_id: str = Field(..., description="Уникальный глобальный идентификатор пользователя (Primary Key)")
    churn_probability: float = Field(..., ge=0.0, le=1.0, description="Вероятность оттока клиента из модели CatBoostClassifier")
    log_date: str = Field(..., description="Дата логирования и батч-партиционирования предикта в формате YYYY-MM-DD")


class CustomerFrustrationPassport(StrictBaseModel):
    user_id: str = Field(..., description="Уникальный глобальный идентификатор пользователя в системе ритейла")
    calculated_ltv_at_risk: float = Field(..., ge=0.0, description="Вычисленная в Greenplum MPP упущенная маржа с учетом когорты удержания (lost_margin_risk в руб)")
    frustration_index: float = Field(..., ge=0.0, description="Рассчитанный в transformer.py на CPU нелинейный индекс фрустрации клиента")
    hot_product_context: str = Field(..., description="Горячий контекст товара, на котором споткнулся пользователь (идентификатор товара из argMax)")
    risk_group: str = Field(..., description="Тег группы риска для автоматизации (CRITICAL_ALERT_VIP_SAVE, HIGH_RISK_SEARCH_STUCK, HIGH_RISK_TECH_FAIL)")
    max_compensation_limit: float = Field(..., ge=0.0, description="Максимальный лимит ручной моментальной компенсации и кэшбэка оператором саппорта (руб)")
    recommended_script_id: str = Field(..., description="Идентификатор целевого интерактивного скрипта удержания для оператора колл-центра")
