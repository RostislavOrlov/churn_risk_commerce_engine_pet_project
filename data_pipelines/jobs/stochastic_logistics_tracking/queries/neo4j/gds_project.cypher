CALL gds.graph.project(
    $graph_id,
    ['LogisticsNode', 'SKU_Package', 'ScanEvent', 'WarehouseWorker', 'LinehaulDriver', 'LastMileCourier'],
    {
        LOGISTICS_LEG: {
            orientation: 'UNDIRECTED',
            properties: {
                base_time: { property: 'base_time', defaultValue: 1.0 }
            }
        },
        TRACKED_BY: {
            orientation: 'UNDIRECTED',
            properties: {
                base_time: { property: 'base_time', defaultValue: 0.1 }
            }
        },
        AT_LOCATION: {
            orientation: 'UNDIRECTED',
            properties: {
                base_time: { property: 'base_time', defaultValue: 0.1 }
            }
        },
        EXECUTED_BY: {
            orientation: 'UNDIRECTED',
            properties: {
                base_time: { property: 'base_time', defaultValue: 0.1 }
            }
        }
    }
)
YIELD graphName, nodeCount, relationshipCount, projectMillis
