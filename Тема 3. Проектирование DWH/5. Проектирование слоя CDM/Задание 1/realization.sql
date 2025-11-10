DROP TABLE IF EXISTS cdm.dm_settlement_report;

CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id INT GENERATED ALWAYS AS IDENTITY,
    restaurant_id VARCHAR NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date DATE NOT NULL,
    orders_count INT NOT NULL,
    orders_total_sum NUMERIC (14, 2) NOT NULL,
    orders_bonus_payment_sum NUMERIC (14, 2) NOT NULL,
    orders_bonus_granted_sum NUMERIC (14, 2) NOT NULL,
    order_processing_fee NUMERIC (14, 2) NOT NULL,
    restaurant_reward_sum NUMERIC (14, 2) NOT NULL
);