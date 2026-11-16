CALL gds.leiden.stream(
    $graph_id,
    {
        relationshipWeightProperty: 'base_time',
        includeIntermediateCommunities: false
    }
)
YIELD nodeId, communityId
WITH gds.util.asNode(nodeId) AS vertex, communityId
WHERE vertex:ScanEvent
WITH communityId,
     COUNT(vertex) AS events_in_community,
     collect(id(vertex)) AS community_event_ids,
     MIN(vertex.timestamp) AS min_timestamp,
     MAX(vertex.timestamp) AS max_timestamp
WHERE events_in_community > 2
WITH communityId,
     community_event_ids,
     (max_timestamp - min_timestamp) / 60.0 AS stage_duration_minutes
WHERE stage_duration_minutes > 15.0
UNWIND community_event_ids AS ev_id
MATCH (node:LogisticsNode)<-[:AT_LOCATION]-(ev:ScanEvent)
WHERE id(ev) = ev_id
SET node.is_bottleneck_flag = 1,
    node.bottleneck_risk_factor = toFloat(1.0 + (stage_duration_minutes / 60.0))
WITH DISTINCT node, stage_duration_minutes
OPTIONAL MATCH (ev2:ScanEvent)-[:AT_LOCATION]->(node)
OPTIONAL MATCH (ev2)-[:EXECUTED_BY]->(worker)
WHERE worker:WarehouseWorker OR worker:LastMileCourier OR worker:LinehaulDriver
SET worker.shift_status = 'FRUSTRATED',
    worker.calculated_delay = stage_duration_minutes
RETURN node.id AS compromised_node_id,
       node.bottleneck_risk_factor AS congestion_level,
       count(worker) AS affected_personnel_count
