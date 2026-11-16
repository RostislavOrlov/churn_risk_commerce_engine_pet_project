import asyncio
import logging
from neo4j import AsyncDriver

from ..config import settings
from ..models.graph_entities import GraphOrderTrackingStatusModel, GraphNodeHealthModel
from .base import BaseNeo4jRepository

logger = logging.getLogger("rtc_neo4j_repo")


class Neo4jGraphRepository(BaseNeo4jRepository):
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    async def fetch_order_live_tracking(self, order_id: str) -> GraphOrderTrackingStatusModel:
        query = """
        MATCH (o:Order {id: $order_id})-[:CONTAINS_PACKAGE]->(pkg:SKU_Package)-[:TRACKED_BY]->(ev:ScanEvent)-[:AT_LOCATION]->(node:LogisticsNode)
        WITH o.id AS target_order_id, pkg, node, ev
        ORDER BY ev.timestamp DESC
        WITH target_order_id,
             pkg.id AS pkg_id,
             pkg.product_id AS product_id,
             head(collect(node)) AS currentNode,
             head(collect(ev)) AS latest_ev

        WITH target_order_id, product_id, currentNode,
             CASE latest_ev.status
                 WHEN 'WH_RECEIVE'       THEN 1
                 WHEN 'WH_DISPATCH'      THEN 2
                 WHEN 'CROSSDOCK_ARRIVE' THEN 3
                 WHEN 'CROSSDOCK_DEPART' THEN 4
                 WHEN 'HUB_ARRIVE'       THEN 5
                 WHEN 'HUB_SORT'         THEN 6
                 WHEN 'PVZ_ARRIVE'       THEN 7
                 WHEN 'CUSTOMER_PICKED'  THEN 8
                 ELSE 1
             END AS current_stage_idx

        OPTIONAL MATCH (currentNode)-[leg:LOGISTICS_LEG]->(nextNode:LogisticsNode)
        WITH target_order_id, product_id, current_stage_idx, currentNode,
             head(collect(leg)) AS next_leg,
             head(collect(nextNode)) AS next_node

        WITH target_order_id, product_id, current_stage_idx, currentNode,
             COALESCE(next_leg.base_time, 1.5) AS leg_time,
             COALESCE(next_leg.congestion, 1.0) AS leg_congestion,
             COALESCE(currentNode.bottleneck_risk_factor, 1.0) AS cur_risk,
             COALESCE(next_node.bottleneck_risk_factor, 1.0) AS next_risk

        WITH target_order_id, product_id, current_stage_idx, currentNode, cur_risk,
             ((leg_time * leg_congestion * cur_risk * next_risk) - leg_time) * 60.0 AS box_delay_minutes

        WITH target_order_id, product_id, current_stage_idx, currentNode, cur_risk,
             CASE WHEN box_delay_minutes > 0.0 THEN box_delay_minutes ELSE 0.0 END AS clean_delay_minutes
        ORDER BY clean_delay_minutes DESC, cur_risk DESC
        LIMIT 1

        RETURN target_order_id AS order_id,
               product_id,
               toFloat(clean_delay_minutes) AS calculated_delay_minutes,
               toInteger(current_stage_idx) AS current_logistic_stage,
               toInteger(COALESCE(currentNode.is_bottleneck_flag, 0)) AS is_bottleneck_flag,
               toFloat(cur_risk) AS bottleneck_risk_factor,
               currentNode.id AS failing_node_id
        """

        async def _run_query():
            async with self.driver.session(database="neo4j") as session:
                res = await session.run(query, order_id=order_id)
                record = await res.single(strict=False)
                if not record:
                    return GraphOrderTrackingStatusModel(order_id=order_id)

                return GraphOrderTrackingStatusModel(
                    order_id=order_id,
                    product_id=str(record.get("product_id") or ""),
                    current_logistic_stage=int(record.get("current_logistic_stage") or 1),
                    calculated_delay_minutes=round(float(record.get("calculated_delay_minutes") or 0.0), 2),
                    is_bottleneck_flag=int(record.get("is_bottleneck_flag") or 0),
                    failing_node_id=str(record.get("failing_node_id") or ""),
                    bottleneck_risk_factor=float(record.get("bottleneck_risk_factor") or 0.0),
                )

        return await asyncio.wait_for(_run_query(), timeout=settings.NEO4J_TIMEOUT_SEC)


    async def fetch_node_health_metrics(self, node_id: str) -> GraphNodeHealthModel:
        query = """
        MATCH (n:LogisticsNode {id: $node_id})
        OPTIONAL MATCH (ev:ScanEvent)-[:AT_LOCATION]->(n)
        OPTIONAL MATCH (ev)-[:EXECUTED_BY]->(worker)
        WHERE worker:WarehouseWorker OR worker:LastMileCourier OR worker:LinehaulDriver
        RETURN coalesce(n.status, 'ACTIVE') AS status,
               coalesce(n.is_bottleneck_flag, 0) AS is_bottleneck_flag,
               coalesce(n.bottleneck_risk_factor, 0.0) AS bottleneck_risk_factor,
               count(DISTINCT worker) AS affected_personnel_count
        """

        async def _run_query():
            async with self.driver.session(database="neo4j") as session:
                res = await session.run(query, node_id=node_id)
                record = await res.single()
                if not record:
                    return GraphNodeHealthModel(node_id=node_id)

                return GraphNodeHealthModel(
                    node_id=node_id,
                    status=str(record.get("status") or "ACTIVE"),
                    is_bottleneck_flag=int(record.get("is_bottleneck_flag") or 0),
                    bottleneck_risk_factor=float(record.get("bottleneck_risk_factor") or 0.0),
                    affected_personnel_count=int(record.get("affected_personnel_count") or 0),
                )

        return await asyncio.wait_for(_run_query(), timeout=settings.NEO4J_TIMEOUT_SEC)
