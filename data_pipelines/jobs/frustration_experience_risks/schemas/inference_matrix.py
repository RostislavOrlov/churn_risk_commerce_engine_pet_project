from pydantic import Field
from .base import StrictBaseModel


class ModelInputFeaturesMatrix(StrictBaseModel):
    user_id: str = Field(..., description="Глобальный сквозной идентификатор клиента платформы")
    product_id: str = Field(..., description="Идентификатор целевого товара горячего контекста намерения")
    
    max_session_depth: int = Field(..., description="Максимальная глубина сессии пользователя за расчетные сутки")
    p90_frustration: float = Field(..., description="90-й процентиль технической фрустрации по временным дельтам кликов")
    total_rage_clicks: int = Field(..., description="Суммарное количество Rage Clicks (серия быстрых кликов < 250 мс)")
    pogo_sticking_count: int = Field(..., description="Количество паттернов челночных возвратов Pogo-Sticking")
    max_velocity_volatility: float = Field(..., description="Максимальная волатильность (отклонение ускорения) скорости взаимодействия")
    max_unique_products: int = Field(..., description="Максимальное число уникальных идентификаторов товаров в рамках сессии")
    max_unique_categories: int = Field(..., description="Максимальное число уникальных категорий товаров, просмотренных за сутки")
    total_perf_events: int = Field(..., description="Суммарное количество логов производительности фронтенда приложения")
    api_errors_count: int = Field(..., description="Количество зафиксированных ошибок API со статус-кодом >= 500")
    p95_interface_render_ms: float = Field(..., description="95-й процентиль задержки отрисовки интерфейса на клиенте")
    avg_network_ms: float = Field(..., description="Средний сетевой лаг передачи пакетов данных и DNS-разрешения в мс")
    total_searches: int = Field(..., description="Общее количество поисковых запросов и сессий пользователя в каталоге")
    total_filter_resets: int = Field(..., description="Количество ручных сбросов или резких очисток фильтров каталога")
    max_search_scroll: int = Field(..., description="Максимальная глубина скролла поисковой выдачи в элементах листинга")
    
    route_cost: float = Field(..., description="Оптимальная финансовая стоимость трассы перемещения товара в рублях")
    route_time: float = Field(..., description="Реальное расчетное время доставки с учетом штрафов ворот хабов в часах")
    route_jitter: float = Field(..., description="Дисперсия стабильности и предсказуемости временных окон трассы логистики")
    bottleneck_risk: float = Field(..., description="Фактор критической утилизации и перегрузки узлов инфраструктуры цепи")
    graph_hops: int = Field(..., description="Количество транзитных физических хабов и кросс-доков в цепочке поставки")
    isolation_score: float = Field(..., description="Коэффициент локальной кластеризации ПВЗ для оценки уязвимости снабжения")
    
    promo_price: float = Field(..., description="Честная промо-цена товара со скидками на расчетную дату от С++ Painless")
    marketing_tags_count: int = Field(..., description="Количество активных маркетинговых тегов на карточке товара")
    has_bundles: int = Field(..., description="Бинарный флаг наличия товара в активных пакетных предложениях (0 или 1)")
    
    frustration_index: float = Field(..., ge=0.0, description="Нелинейный индекс фрустрации клиента, рассчитанный на CPU в трансформере")
