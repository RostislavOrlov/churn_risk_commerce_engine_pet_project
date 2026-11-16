CALL gds.graph.project(
    $graph_id,
    'LogisticsNode',
    {
        LOGISTICS_LEG: {
            orientation: 'NATURAL',
            properties: ['base_time', 'congestion']
        }
    }
)

YIELD graphName, nodeCount, relationshipCount;
