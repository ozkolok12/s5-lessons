
/*COPY dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to) FROM '/data/dm_restaurants_';
COPY dds.dm_products (id, restaurant_id, product_id, product_name, product_price, active_from, active_to) FROM '/data/dm_products_';
COPY dds.dm_timestamps (id, ts, year, month, day, "time", date) FROM '/data/dm_timestamps_';
COPY dds.dm_users (id, user_id, user_name, user_login) FROM '/data/dm_users_';
COPY dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id) FROM '/data/dm_orders_';
COPY dds.fct_product_sales (id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant) FROM '/data/fct_product_sales_';
*/

INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
  )
SELECT 
    o.restaurant_id,
    r.restaurant_name,
    t.date,
    COUNT(DISTINCT f.order_id),
    SUM(f.total_sum),
    SUM(f.bonus_payment),
    SUM(f.bonus_grant),
    SUM(f.total_sum * 0.25),
    (SUM(f.total_sum * 0.75) - SUM(f.bonus_payment))
  
FROM dds.dm_orders o

JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
JOIN dds.fct_product_sales f ON o.id = f.order_id

WHERE o.order_status = 'CLOSED'
GROUP BY o.restaurant_id, r.restaurant_name, t.date

ON CONFLICT (restaurant_id, settlement_date)
DO UPDATE SET
    restaurant_name = EXCLUDED.restaurant_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;

--SELECT * FROM cdm.dm_settlement_report LIMIT 10;

--SELECT DISTINCT order_status FROM dds.dm_orders; 