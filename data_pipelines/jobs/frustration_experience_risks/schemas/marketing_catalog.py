from pydantic import Field
from .base import StrictBaseModel


class ElasticsearchMarketingContext(StrictBaseModel):
    promo_price: float = Field(default=0.0, ge=0.0, description="Честная динамическая промо-цена со всеми скидками на расчетную дату, вычисленная C++ Painless")
    category_path: str = Field(default="UNKNOWN", description="Полный хлебный путь (breadcrumbs) текстовой иерархии категорий товара, разделенный знаком '>'")
    leaf_category_id: int = Field(default=0, ge=0, description="Идентификатор конечной (листовой) категории в номенклатурном справочнике ERP-системы")
    marketing_tags_count: int = Field(default=0, ge=0, description="Количество активных маркетинговых тегов на товаре (например: HIT, EXCLUSIVE, NEW)")
    has_bundles: int = Field(default=0, ge=0, le=1, description="Бинарный флаг наличия товара в active-маркетинговых бандлах и пакетных предложениях (0 или 1)")
