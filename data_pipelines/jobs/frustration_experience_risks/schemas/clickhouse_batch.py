from pydantic import Field
from .base import StrictBaseModel


class ClickHouseChunkRow(StrictBaseModel):
    user_id: str = Field(..., description="Уникальный идентификатор пользователя (Primary Key воркера, распределенный хэш-ключ)")
    max_session_depth: int = Field(..., ge=0, description="Максимальная глубина сессии пользователя за сутки (количество просмотренных страниц/событий)")
    p90_frustration: float = Field(..., ge=0.0, description="90-й процентиль технической фрустрации по временным дельтам кликов пользователя за сутки")
    total_rage_clicks: int = Field(..., ge=0, description="Суммарное количество Rage Clicks (серия яростных кликов с интервалом менее 250 мс)")
    pogo_sticking_count: int = Field(..., ge=0, description="Количество зафиксированных паттернов челночных возвратов Pogo-Sticking (каталог -> карточка -> каталог)")
    max_velocity_volatility: float = Field(..., description="Максимальная волатильность (стандартное отклонение ускорения) скорости взаимодействия пользователя с UI")
    max_unique_products: int = Field(..., ge=0, description="Максимальное число уникальных идентификаторов товаров в рамках одной сессии (Cardinality)")
    max_unique_categories: int = Field(..., ge=0, description="Максимальное число уникальных категорий товаров, просмотренных пользователем за сутки")
    total_perf_events: int = Field(..., ge=0, description="Суммарное количество логов и системных перформанс-событий производительности фронтенда")
    api_errors_count: int = Field(..., ge=0, description="Количество зафиксированных критических ошибок API (сетевые коды ответов status_code >= 500)")
    p95_interface_render_ms: float = Field(..., ge=0.0, description="95-й процентиль задержки отрисовки и рендеринга интерфейса приложения на клиенте (мс)")
    avg_network_ms: float = Field(..., ge=0.0, description="Средний сетевой лаг передачи пакетов данных и DNS-разрешения (Network Latency в мс)")
    total_searches: int = Field(..., ge=0, description="Общее количество поисковых запросов и фасетных сессий пользователя в каталоге ритейла")
    total_filter_resets: int = Field(..., ge=0, description="Количество ручных сбросов, замен или резких очисток фильтров каталога (маркер фрустрации поиска)")
    max_search_scroll: int = Field(..., ge=0, description="Максимальная глубина скролла поисковой выдачи, зафиксированная в элементах листинга товаров")
    warehouse_id: str = Field(..., description="ID исходного логистического склада отгрузки для последнего контекста намерения пользователя")
    pickup_point_id: str = Field(..., description="ID целевого пункта выдачи заказов (ПВЗ) доставки для последнего контекста намерения пользователя")
    product_id: str = Field(..., description="Горячий контекст намерения: последний просмотренный пользователем товар, вычисленный через argMax")
