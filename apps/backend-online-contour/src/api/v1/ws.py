from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from redis.asyncio import Redis

from ...repositories.postgres_client import pg_manager, PostgresAnalyticsRepository
from ...repositories.redis_client import RedisFeatureRepository

logger = logging.getLogger("rtc_websocket")
router = APIRouter(tags=["Real-Time Control Tower Stream"])


class ControlTowerConnectionManager:
    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"[WS] Диспетчер подключился. Активных экранов: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket) -> None:
        self.active_connections.discard(websocket)
        logger.info(f"[WS] Диспетчер отключился. Активных экранов: {len(self.active_connections)}")

    async def broadcast_event(self, event_type: str, payload: dict) -> None:
        """Мгновенная рассылка события (например, REROUTE или VOUCHER_ISSUED) всем диспетчерам."""
        if not self.active_connections:
            return
        message = json.dumps(
            {
                "type": event_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": payload,
            },
            default=str,
        )
        dead_sockets = []
        for ws in list(self.active_connections):
            try:
                await ws.send_text(message)
            except Exception:
                dead_sockets.append(ws)
        for ws in dead_sockets:
            self.disconnect(ws)


ws_manager = ControlTowerConnectionManager()


async def _collect_live_snapshot() -> dict:
    """Собирает срез активных инцидентов и заблокированных узлов для стриминга по WebSocket."""
    from ...main import db_state

    pg_repo = PostgresAnalyticsRepository()
    incidents_data = []
    nodes_data = []

    if db_state.redis_pool:
        r_client = Redis(connection_pool=db_state.redis_pool)
        try:
            redis_repo = RedisFeatureRepository(r_client)
            cached_inc = await redis_repo.get_active_incidents_cached()
            cached_nodes = await redis_repo.get_blocked_nodes_cached()
            if cached_inc is not None:
                incidents_data = cached_inc
            if cached_nodes is not None:
                nodes_data = cached_nodes
        except Exception as err:
            logger.warning(f"[WS][REDIS_SNAPSHOT_WARN] {err}")
        finally:
            await r_client.aclose()

    # Если кэш пуст, добираем с Postgres Replica
    if (not incidents_data or not nodes_data) and pg_manager.replica_session_factory:
        try:
            async with pg_manager.replica_session_factory() as session:
                if not incidents_data:
                    rows = await pg_repo.fetch_active_incidents(session, limit=25)
                    incidents_data = [
                        {
                            "incident_id": str(r.incident_id),
                            "order_id": r.order_id,
                            "failing_node_id": r.failing_node_id,
                            "logistic_stage_idx": int(r.logistic_stage_idx),
                            "calculated_delay_minutes": float(r.calculated_delay_minutes),
                            "incident_status": r.incident_status,
                            "assigned_team": r.assigned_team,
                        }
                        for r in rows
                    ]
                if not nodes_data:
                    n_rows = await pg_repo.fetch_blocked_or_risky_nodes(session, risk_threshold=0.5)
                    nodes_data = [
                        {
                            "node_id": n.node_id,
                            "node_type": n.node_type,
                            "base_capacity_packages": int(n.base_capacity_packages),
                            "current_capacity_packages": int(n.current_capacity_packages),
                            "bottleneck_risk_factor": float(n.bottleneck_risk_factor),
                            "is_active_block": int(n.is_active_block),
                        }
                        for n in n_rows
                    ]
        except Exception as db_err:
            logger.warning(f"[WS][DB_SNAPSHOT_WARN] {db_err}")

    return {
        "type": "TELEMETRY_SNAPSHOT",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "incidents": incidents_data,
        "restricted_nodes": nodes_data,
    }


@router.websocket("/ws/control-tower")
async def control_tower_stream(websocket: WebSocket) -> None:
    await ws_manager.connect(websocket)
    try:
        while True:
            snapshot = await _collect_live_snapshot()
            await websocket.send_text(json.dumps(snapshot, default=str))
            await asyncio.sleep(3.0)
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as exc:
        logger.error(f"[WS][STREAM_ERROR] {exc}")
        ws_manager.disconnect(websocket)
