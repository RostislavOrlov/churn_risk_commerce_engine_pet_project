MATCH (origin)-[r:DELIVERY_CORRIDOR]->(pp:PickupPoint {status: 'ACTIVE'})
WHERE origin:Warehouse OR origin:Darkstore OR origin:SellerWarehouse

WITH r, origin, pp, r.transit_nodes_chain AS chain, r.base_link_times AS times_vector,
     toFloat(datetime().hour) + (toFloat(datetime().minute) / 60.0) AS start_hour
MATCH (transit:LogisticsNode)
WHERE transit.id IN chain

WITH r, origin, pp, times_vector, start_hour, transit,
     [i IN range(0, size(chain)-1) WHERE chain[i] = transit.id] AS node_index
ORDER BY node_index
WITH r, origin, pp, start_hour, times_vector, collect(transit) AS ordered_nodes_list

WITH r, origin, pp, start_hour,
     [idx IN range(0, size(ordered_nodes_list)-1) | {
         leg_base_time: toFloat(times_vector[idx]),
         congestion: toFloat(COALESCE(ordered_nodes_list[idx].bottleneck_risk_factor, 1.0)),
         open_hour: toFloat(COALESCE(ordered_nodes_list[idx].open_hour, 0.0)),
         close_hour: toFloat(COALESCE(ordered_nodes_list[idx].close_hour, 24.0))
     }] AS steps

WITH r, origin, pp, start_hour,
     REDUCE(
         acc = {current_time: start_hour, total_delay: 0.0, max_load: 0.0},
         s IN steps |
         {
             current_time: (acc.current_time + s.leg_base_time * s.congestion) + (
                 CASE
                     WHEN ((acc.current_time + s.leg_base_time * s.congestion) % 24) > s.close_hour
                     THEN (24.0 - ((acc.current_time + s.leg_base_time * s.congestion) % 24)) + s.open_hour
                     WHEN ((acc.current_time + s.leg_base_time * s.congestion) % 24) < s.open_hour
                     THEN s.open_hour - ((acc.current_time + s.leg_base_time * s.congestion) % 24)
                     ELSE 0.0
                 END
             ),
             total_delay: acc.total_delay + ((s.congestion - 1.0) * s.leg_base_time * 60.0) + (
                 CASE
                     WHEN ((acc.current_time + s.leg_base_time * s.congestion) % 24) > s.close_hour
                     THEN ((24.0 - ((acc.current_time + s.leg_base_time * s.congestion) % 24)) + s.open_hour) * 60.0
                     WHEN ((acc.current_time + s.leg_base_time * s.congestion) % 24) < s.open_hour
                     THEN (s.open_hour - ((acc.current_time + s.leg_base_time * s.congestion) % 24)) * 60.0
                     ELSE 0.0
                 END
             ),
             max_load: CASE WHEN s.congestion > acc.max_load THEN s.congestion ELSE acc.max_load END
         }
     ) AS sim_res

WITH r, origin, pp, start_hour, sim_res, 
     (sim_res.current_time - start_hour) - r.base_time AS delay_delta

RETURN 
    origin.id AS origin_node_id,
    pp.id AS pickup_point_id,
    toFloat(sim_res.current_time - start_hour) AS route_time,
    toFloat(r.base_cost) AS route_cost,
    toFloat(COALESCE(abs(delay_delta), 0.0)) AS route_jitter,
    toFloat(sim_res.max_load) AS bottleneck_risk_factor,
    toInteger(size(r.transit_nodes_chain)) AS graph_hops,
    toFloat(COALESCE(pp.local_clustering_coef, 0.0)) AS isolation_score
