UNWIND $batch AS row
MERGE (c:Customer {id: 'CUST_' + row.user_id})

MERGE (o:Order {id: row.order_id})
ON CREATE SET o.amount = toFloat(row.amount), o.status = row.status
ON MATCH SET o.amount = toFloat(row.amount), o.status = row.status

MERGE (c)-[r:PLACED_ORDER]->(o)
ON CREATE SET r.created_at = timestamp()
ON MATCH SET r.updated_at = timestamp()
RETURN count(o) AS imported_count;
