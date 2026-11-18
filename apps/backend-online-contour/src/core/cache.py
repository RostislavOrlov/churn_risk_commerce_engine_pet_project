# TODO: Заготовка под двухуровневый кэш (L1 RAM + L2 Redis) при росте нагрузки.
# Пока используем репозиторий redis_client

import time
import json
import hmac
import hashlib
import logging
from typing import Any, Optional, Dict, Tuple
from redis.asyncio import Redis

from ..config import settings
from .metrics import CACHE_OPERATIONS_TOTAL

logger = logging.getLogger("rtc_cache_engine")


class HighLoadCacheEngine:
    def __init__(self, redis_client: Optional[Redis] = None, max_l1_entries: int = 5000):
        self.redis = redis_client
        self.l1_memory_store: Dict[str, Tuple[Any, float]] = {}
        self.l1_ttl = settings.CACHE_L1_TTL_SEC
        self.l2_ttl = settings.CACHE_L2_TTL_SEC
        self.max_l1_entries = max_l1_entries
        self._secret_key = b"realtime_commerce_secure_cache_salt_v2"

    def generate_secure_key(self, namespace: str, unique_identifier: str) -> str:
        raw_key = f"rtc_cache:{namespace}:{unique_identifier}".encode("utf-8")
        signature = hmac.new(self._secret_key, raw_key, hashlib.sha256).hexdigest()
        return f"rtc:cache:{namespace}:{unique_identifier}:{signature[:12]}"

    def get_l1(self, key: str) -> Optional[Any]:
        entry = self.l1_memory_store.get(key)
        if entry is not None:
            data, expires_at = entry
            if time.time() < expires_at:
                return data
            self.l1_memory_store.pop(key, None)
        return None

    def set_l1(self, key: str, data: Any) -> None:
        if len(self.l1_memory_store) >= self.max_l1_entries:
            self.clear_expired_l1_garbage()
            if len(self.l1_memory_store) >= self.max_l1_entries:
                oldest_key = next(iter(self.l1_memory_store))
                self.l1_memory_store.pop(oldest_key, None)
        self.l1_memory_store[key] = (data, time.time() + self.l1_ttl)

    async def get_l2(self, key: str) -> Optional[Any]:
        if not self.redis:
            return None
        try:
            raw_data = await self.redis.get(key)
            if raw_data:
                decoded = raw_data.decode("utf-8") if isinstance(raw_data, bytes) else raw_data
                return json.loads(decoded)
        except Exception as e:
            logger.error(f"[CACHE_ENGINE][L2_READ_FAIL] Ошибка чтения из Redis: {e}")
        return None

    async def set_l2(self, key: str, data: Any, ttl: Optional[int] = None) -> None:
        if not self.redis:
            return
        try:
            serialized_data = json.dumps(data, default=str)
            await self.redis.set(key, serialized_data, ex=ttl or self.l2_ttl)
        except Exception as e:
            logger.error(f"[CACHE_ENGINE][L2_WRITE_FAIL] Ошибка записи в Redis: {e}")

    async def get_hybrid(self, namespace: str, unique_identifier: str) -> Optional[Any]:
        secure_key = self.generate_secure_key(namespace, unique_identifier)

        l1_hit = self.get_l1(secure_key)
        if l1_hit is not None:
            CACHE_OPERATIONS_TOTAL.labels(namespace=namespace, layer="L1", result="HIT").inc()
            return l1_hit

        l2_hit = await self.get_l2(secure_key)
        if l2_hit is not None:
            CACHE_OPERATIONS_TOTAL.labels(namespace=namespace, layer="L2", result="HIT").inc()
            self.set_l1(secure_key, l2_hit)
            return l2_hit

        CACHE_OPERATIONS_TOTAL.labels(namespace=namespace, layer="L1_L2", result="MISS").inc()
        return None

    async def set_hybrid(
        self, namespace: str, unique_identifier: str, data: Any, l2_ttl: Optional[int] = None
    ) -> None:
        secure_key = self.generate_secure_key(namespace, unique_identifier)
        self.set_l1(secure_key, data)
        await self.set_l2(secure_key, data, ttl=l2_ttl)

    async def invalidate_hybrid(self, namespace: str, unique_identifier: str) -> None:
        secure_key = self.generate_secure_key(namespace, unique_identifier)
        self.l1_memory_store.pop(secure_key, None)
        if self.redis:
            try:
                await self.redis.delete(secure_key)
            except Exception as e:
                logger.error(f"[CACHE_ENGINE][INVALIDATE_FAIL] Ошибка удаления ключа из Redis: {e}")

    def clear_expired_l1_garbage(self) -> int:
        current_time = time.time()
        expired_keys = [k for k, (_, exp) in self.l1_memory_store.items() if current_time >= exp]
        for k in expired_keys:
            self.l1_memory_store.pop(k, None)
        return len(expired_keys)
