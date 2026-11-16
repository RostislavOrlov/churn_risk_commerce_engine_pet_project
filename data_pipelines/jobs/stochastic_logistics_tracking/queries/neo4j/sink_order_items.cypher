MERGE (o:Order {id: event.order_id})
MERGE (pkg:SKU_Package {id: event.product_id})
MERGE (o)-[r:CONTAINS_PACKAGE]->(pkg)
