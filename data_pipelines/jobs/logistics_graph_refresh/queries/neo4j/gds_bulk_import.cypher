WITH event AS row,
     coalesce(event.darkstore_id, event.location_id) AS loc_id,
     coalesce(event.executor_id, event.worker_id) AS exec_id

MERGE (ev:ScanEvent {id: row.event_id})
SET ev.timestamp = toInteger(row.timestamp),
    ev.status = row.status

WITH ev, row, loc_id, exec_id
OPTIONAL MATCH (pkg:SKU_Package {id: row.package_id})
FOREACH (p IN CASE WHEN pkg IS NOT NULL THEN [pkg] ELSE [] END |
    MERGE (p)-[:TRACKED_BY]->(ev)
)

WITH ev, pkg, loc_id, exec_id
OPTIONAL MATCH (node:LogisticsNode {id: loc_id})
FOREACH (n IN CASE WHEN node IS NOT NULL THEN [node] ELSE [] END |
    MERGE (ev)-[:AT_LOCATION]->(n)
)

WITH ev, pkg, exec_id
OPTIONAL MATCH (executor)
WHERE executor.id = exec_id
  AND (executor:WarehouseWorker OR executor:LinehaulDriver OR executor:LastMileCourier)
FOREACH (ex IN CASE WHEN executor IS NOT NULL THEN [executor] ELSE [] END |
    MERGE (ev)-[:EXECUTED_BY]->(ex)
)

WITH ev, pkg
WHERE pkg IS NOT NULL
OPTIONAL MATCH (pkg)-[:TRACKED_BY]->(prev_ev:ScanEvent)
WHERE prev_ev.timestamp < ev.timestamp
WITH ev, prev_ev
ORDER BY prev_ev.timestamp DESC
WITH ev, head(collect(prev_ev)) AS last_prev_ev

FOREACH (p IN CASE WHEN last_prev_ev IS NOT NULL THEN [last_prev_ev] ELSE [] END |
    MERGE (p)-[tr:TRANSIT]->(ev)
    SET tr.duration_seconds = ev.timestamp - p.timestamp
)
