CREATE TABLE dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR UNIQUE,
    courier_name VARCHAR
);

CREATE TABLE dds.fct_deliveries (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    courier_id VARCHAR NOT NULL,
    order_ts TIMESTAMP,
    rate FLOAT,
    tip_sum NUMERIC(14,2)
);