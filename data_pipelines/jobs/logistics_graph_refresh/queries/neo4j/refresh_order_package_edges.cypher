UNWIND $batch AS row
MATCH (o:Order {id: row.order_id})

MERGE (pkg:SKU_Package {id: row.package_id})
ON CREATE SET pkg.type = row.package_type, pkg.weight = toFloat(row.weight)
ON MATCH SET pkg.type = row.package_type, pkg.weight = toFloat(row.weight)

MERGE (o)-[r:CONTAINS_PACKAGE]->(pkg)
ON MATCH SET r.last_verified_at = timestamp()
RETURN count(pkg) AS imported_count;
