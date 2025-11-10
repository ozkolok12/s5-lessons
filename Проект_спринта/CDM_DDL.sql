CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id ALWAYS GENERATED AS IDENTITY  PRIMARY KEY,

    courier_id VARCHAR NOT NULL,       
    courier_name VARCHAR NOT NULL,     

    settlement_year INT NOT NULL,      
    settlement_month INT NOT NULL,     

    orders_count INT NOT NULL,         
    orders_total_sum NUMERIC(14, 2) NOT NULL,

    rate_avg FLOAT,                     
    order_processing_fee NUMERIC(14, 2),

    courier_order_sum NUMERIC(14, 2),   
    courier_tips_sum NUMERIC(14, 2),    
    courier_reward_sum NUMERIC(14, 2)   
);