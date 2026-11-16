from pydantic import Field
from .base import StrictBaseModel


class Neo4jLogisticsContext(StrictBaseModel):
    route_cost: float = Field(default=50.0, ge=0.0, description="Оптимальная финансовая стоимость трассы перемещения товара (базовый тариф цепи поставок в руб)")
    route_time: float = Field(default=24.0, ge=0.0, description="Реальное расчетное время доставки с учетом штрафов открытия/закрытия ворот промежуточных хабов (ч)")
    route_jitter: float = Field(default=0.0, ge=0.0, description="Дисперсия (нестабильность) и предсказуемости временных окон трассы на основе топ-3 альтернативных путей")
    bottleneck_risk: float = Field(default=0.0, ge=0.0, description="Фактор критической утилизации и перегрузки узлов инфраструктуры цепи поставок (max_node_load)")
    graph_hops: int = Field(default=2, ge=0, description="Количество транзитных физических хабов, кросс-доков и сортировочных центров в цепочке поставки")
    isolation_score: float = Field(default=0.0, ge=0.0, description="Коэффициент локальной кластеризации ПВЗ в графе для оценки логистической уязвимости снабжения коридора")
