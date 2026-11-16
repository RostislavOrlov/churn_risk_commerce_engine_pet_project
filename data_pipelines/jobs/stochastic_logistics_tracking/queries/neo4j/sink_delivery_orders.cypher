MERGE (o:Order {id: event.order_id})
ON CREATE SET o.created_at = timestamp()
ON MATCH SET 
    o.calculated_delay_minutes = toFloat(event.calculated_delay_minutes),
    o.current_logistic_stage = toInteger(event.current_logistic_stage),
    o.is_bottleneck_flag = toInteger(event.is_bottleneck_flag),
    o.updated_at = timestamp()
