MATCH (o:Order)-[:CONTAINS_PACKAGE]->(pkg:SKU_Package)-[:TRACKED_BY]->(ev:ScanEvent)-[:AT_LOCATION]->(node:LogisticsNode)
WITH o.id AS target_order_id, pkg.id AS pkg_id, node, ev
ORDER BY ev.timestamp DESC
WITH target_order_id, pkg_id, head(collect(node)) AS currentNode, head(collect(ev)) AS latest_ev

WITH target_order_id, currentNode,
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
WITH target_order_id, current_stage_idx, currentNode, head(collect(leg)) AS next_leg, head(collect(nextNode)) AS next_node

WITH target_order_id, current_stage_idx, currentNode,
     COALESCE(next_leg.base_time, 1.5) AS leg_time,
     COALESCE(next_leg.congestion, 1.0) AS leg_congestion,
     COALESCE(currentNode.bottleneck_risk_factor, 1.0) AS cur_risk,
     COALESCE(next_node.bottleneck_risk_factor, 1.0) AS next_risk

WITH target_order_id, current_stage_idx, currentNode,
     ((leg_time * leg_congestion * cur_risk * next_risk) - leg_time) * 60.0 AS box_delay_minutes

WITH target_order_id,
     MAX(current_stage_idx) AS final_stage,
     MAX(CASE WHEN box_delay_minutes > 0.0 THEN box_delay_minutes ELSE 0.0 END) AS worst_delay_minutes,
     MAX(COALESCE(currentNode.is_bottleneck_flag, 0)) AS node_bottleneck_status,
     head(collect(currentNode.id)) AS failing_node_id

RETURN target_order_id AS order_id,
       toFloat(worst_delay_minutes) AS calculated_delay_minutes,
       toInteger(final_stage) AS current_logistic_stage,
       toInteger(node_bottleneck_status) AS is_bottleneck_flag,
       failing_node_id
