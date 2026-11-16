CREATE CONSTRAINT unique_logistics_node_id IF NOT EXISTS FOR (n:LogisticsNode) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT unique_warehouse_id IF NOT EXISTS FOR (w:Warehouse) REQUIRE w.id IS UNIQUE;
CREATE CONSTRAINT unique_darkstore_id IF NOT EXISTS FOR (d:Darkstore) REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT unique_seller_wh_id IF NOT EXISTS FOR (s:SellerWarehouse) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT unique_cross_dock_id IF NOT EXISTS FOR (c:CrossDock) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT unique_sorting_hub_id IF NOT EXISTS FOR (s:SortingHub) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT unique_pickup_point_id IF NOT EXISTS FOR (p:PickupPoint) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT unique_customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT unique_order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT unique_package_id IF NOT EXISTS FOR (p:SKU_Package) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT unique_scan_event_id IF NOT EXISTS FOR (e:ScanEvent) REQUIRE e.id IS UNIQUE;

CREATE CONSTRAINT unique_worker_id IF NOT EXISTS FOR (w:WarehouseWorker) REQUIRE w.id IS UNIQUE;
CREATE CONSTRAINT unique_driver_id IF NOT EXISTS FOR (d:LinehaulDriver) REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT unique_courier_id IF NOT EXISTS FOR (c:LastMileCourier) REQUIRE c.id IS UNIQUE;


CREATE INDEX logistics_leg_status_idx IF NOT EXISTS FOR ()-[r:LOGISTICS_LEG]-() ON (r.status, r.allowed_categories);
CREATE INDEX logistics_node_bottleneck_idx IF NOT EXISTS FOR (n:LogisticsNode) ON (n.is_bottleneck_flag, n.bottleneck_risk_factor);
CREATE INDEX pickup_point_status_idx IF NOT EXISTS FOR (p:PickupPoint) ON (p.status, p.current_load_pct);

CREATE INDEX scan_event_stream_idx IF NOT EXISTS FOR (e:ScanEvent) ON (e.timestamp, e.status);
CREATE INDEX package_topology_idx IF NOT EXISTS FOR (p:SKU_Package) ON (p.type);

CREATE INDEX worker_shift_idx IF NOT EXISTS FOR (w:WarehouseWorker) ON (w.role, w.shift);
CREATE INDEX driver_route_idx IF NOT EXISTS FOR (d:LinehaulDriver) ON (d.vehicle_type, d.shift);
CREATE INDEX courier_transport_idx IF NOT EXISTS FOR (c:LastMileCourier) ON (c.transport, c.current_load_orders);
