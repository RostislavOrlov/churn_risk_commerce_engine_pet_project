from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from redis.asyncio import Redis

from ..core.metrics import CACHE_OPERATIONS_TOTAL
from ..models.cache_entities import NodeDynamicCapacityCacheModel, UserOnlineFeaturesModel
from .base import BaseRedisRepository

logger = logging.getLogger("rtc_redis_repo")


def _decode_redis_hash(raw_hash: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode("utf-8") if isinstance(k, bytes) else str(k)): (
            v.decode("utf-8") if isinstance(v, bytes) else v
        )
        for k, v in raw_hash.items()
    }


class RedisFeatureRepository(BaseRedisRepository):
    def __init__(self, redis_client: Redis):
        self.redis = redis_client

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

    async def get_user_online_features(self, user_id: str) -> Optional[UserOnlineFeaturesModel]:
        key = f"online:features:user:{user_id}"
        raw_hash = await self.redis.hgetall(key)
        if not raw_hash:
            CACHE_OPERATIONS_TOTAL.labels(namespace="user_features", layer="L2_REDIS", result="MISS").inc()
            return None

        CACHE_OPERATIONS_TOTAL.labels(namespace="user_features", layer="L2_REDIS", result="HIT").inc()
        data = _decode_redis_hash(raw_hash)
        data.setdefault("user_id", user_id)

        if "marketing_tags_count" in data and "category_depth" not in data:
            data["category_depth"] = data["marketing_tags_count"]

        return UserOnlineFeaturesModel(**data)


    async def get_user_ltv_cached(self, user_id: str) -> Optional[Dict[str, Any]]:
        raw = await self.redis.get(f"online:dwh:user_ltv:{user_id}")
        if not raw:
            CACHE_OPERATIONS_TOTAL.labels(namespace="user_ltv", layer="L2_REDIS", result="MISS").inc()
            return None
        CACHE_OPERATIONS_TOTAL.labels(namespace="user_ltv", layer="L2_REDIS", result="HIT").inc()
        return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)

    async def set_user_ltv_cached(self, user_id: str, payload: Dict[str, Any], ttl_sec: int = 3600) -> None:
        await self.redis.set(f"online:dwh:user_ltv:{user_id}", json.dumps(payload), ex=ttl_sec)

    async def get_market_margin_cached(self, order_id: str) -> Optional[Dict[str, Any]]:
        raw = await self.redis.get(f"online:dwh:market_margin:{order_id}")
        if not raw:
            CACHE_OPERATIONS_TOTAL.labels(namespace="market_margin", layer="L2_REDIS", result="MISS").inc()
            return None
        CACHE_OPERATIONS_TOTAL.labels(namespace="market_margin", layer="L2_REDIS", result="HIT").inc()
        return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)

    async def set_market_margin_cached(self, order_id: str, payload: Dict[str, Any], ttl_sec: int = 1800) -> None:
        await self.redis.set(f"online:dwh:market_margin:{order_id}", json.dumps(payload), ex=ttl_sec)


    async def get_user_compensations_cached(self, user_id: str) -> Optional[List[Dict[str, Any]]]:
        raw = await self.redis.get(f"online:customer:compensations:{user_id}")
        if not raw:
            CACHE_OPERATIONS_TOTAL.labels(namespace="compensations", layer="L2_REDIS", result="MISS").inc()
            return None
        CACHE_OPERATIONS_TOTAL.labels(namespace="compensations", layer="L2_REDIS", result="HIT").inc()
        return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)

    async def set_user_compensations_cached(
        self, user_id: str, items: List[Dict[str, Any]], ttl_sec: int = 60
    ) -> None:
        await self.redis.set(f"online:customer:compensations:{user_id}", json.dumps(items), ex=ttl_sec)

    async def invalidate_user_compensations(self, user_id: str) -> None:
        await self.redis.delete(f"online:customer:compensations:{user_id}")


    async def get_active_incidents_cached(
        self, failing_node_id: Optional[str] = None, limit: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        raw = await self.redis.get("online:incidents:active")
        if not raw:
            CACHE_OPERATIONS_TOTAL.labels(namespace="incidents", layer="L2_REDIS", result="MISS").inc()
            return None

        CACHE_OPERATIONS_TOTAL.labels(namespace="incidents", layer="L2_REDIS", result="HIT").inc()
        items: List[Dict[str, Any]] = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)

        offset = 0
        if failing_node_id and failing_node_id.startswith("offset_"):
            try:
                offset = int(failing_node_id.split("_", 1)[1])
            except ValueError:
                offset = 0
        elif failing_node_id and failing_node_id != "ALL":
            items = [i for i in items if i.get("failing_node_id") == failing_node_id]

        return items[offset : offset + limit]

    async def set_active_incidents_cached(
        self,
        items: List[Dict[str, Any]],
        failing_node_id: Optional[str] = None,
        limit: int = 100,
        ttl_sec: int = 10,
    ) -> None:
        await self.redis.set("online:incidents:active", json.dumps(items, default=str), ex=ttl_sec)

    async def invalidate_active_incidents(self) -> None:
        await self.redis.delete("online:incidents:active")

    async def get_node_capacity_cached(self, node_id: str) -> Optional[NodeDynamicCapacityCacheModel]:
        key = f"online:node:capacity:{node_id}"
        raw_hash = await self.redis.hgetall(key)
        if not raw_hash:
            CACHE_OPERATIONS_TOTAL.labels(namespace="topology", layer="L2_REDIS", result="MISS").inc()
            return None

        CACHE_OPERATIONS_TOTAL.labels(namespace="topology", layer="L2_REDIS", result="HIT").inc()
        data = _decode_redis_hash(raw_hash)
        data.setdefault("node_id", node_id)
        return NodeDynamicCapacityCacheModel(**data)

    async def set_node_capacity_cached(
        self, node_id: str, payload: Dict[str, Any], ttl_sec: int = 30
    ) -> None:
        key = f"online:node:capacity:{node_id}"
        mapped = {k: str(v) for k, v in payload.items() if v is not None}
        if mapped:
            await self.redis.hset(key, mapping=mapped)
            await self.redis.expire(key, ttl_sec)

    async def get_blocked_nodes_cached(self) -> Optional[List[Dict[str, Any]]]:
        key = "online:topology:blocked_nodes"
        raw = await self.redis.get(key)
        if not raw:
            CACHE_OPERATIONS_TOTAL.labels(namespace="topology", layer="L2_REDIS", result="MISS").inc()
            return None

        CACHE_OPERATIONS_TOTAL.labels(namespace="topology", layer="L2_REDIS", result="HIT").inc()
        decoded = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        return json.loads(decoded)

    async def set_blocked_nodes_cached(self, nodes: List[Dict[str, Any]], ttl_sec: int = 30) -> None:
        key = "online:topology:blocked_nodes"
        await self.redis.set(key, json.dumps(nodes, default=str), ex=ttl_sec)
