CREATE TABLE stg.ordersystem_couriers (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR,
    object_value JSONB,
    update_ts TIMESTAMP
);

CREATE TABLE stg.ordersystem_deliveries (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR,
    object_value JSONB,
    update_ts TIMESTAMP
);
