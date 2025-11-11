# cdm_courier_monthly_loader.py
from logging import Logger
from lib import PgConnect

UPSERT_SQL = """
WITH base AS (
  SELECT
      fd.courier_id,
      c.courier_name,
      fd.order_id,                                  -- строковый order_key
      EXTRACT(YEAR  FROM fd.order_ts)::int  AS y,
      EXTRACT(MONTH FROM fd.order_ts)::int AS m,
      fd.rate,
      COALESCE(fd.tip_sum,0)::numeric(14,2) AS tip_sum
  FROM dds.fct_deliveries fd
  JOIN dds.dm_couriers c ON c.courier_id = fd.courier_id
),
order_totals AS (
  /* Сумма каждого заказа (order_total) из fct_product_sales */
  SELECT
      dmo.order_key,
      COALESCE(SUM(fps.total_sum),0)::numeric(14,2) AS order_total
  FROM dds.dm_orders AS dmo
  JOIN dds.fct_product_sales AS fps
    ON fps.order_id = dmo.id
  GROUP BY dmo.order_key
),
r_month AS (
  /* Месячный средний рейтинг r по курьеру */
  SELECT
      courier_id, y, m,
      AVG(rate)::float8 AS r
  FROM base
  GROUP BY courier_id, y, m
),
per_order AS (
  /* Присваиваем каждому заказу total и правила выплаты */
  SELECT
      b.courier_id,
      b.courier_name,
      b.y AS settlement_year,
      b.m AS settlement_month,
      b.order_id,
      ot.order_total,
      b.tip_sum,
      rm.r,
      CASE
        WHEN COALESCE(rm.r, 0) < 4.0 THEN 0.05
        WHEN rm.r < 4.5              THEN 0.07
        WHEN rm.r < 4.9              THEN 0.08
        ELSE                               0.10
      END AS pct,
      CASE
        WHEN COALESCE(rm.r, 0) < 4.0 THEN 100
        WHEN rm.r < 4.5              THEN 150
        WHEN rm.r < 4.9              THEN 175
        ELSE                               200
      END::numeric(14,2) AS min_per_order
  FROM base b
  JOIN order_totals ot
    ON ot.order_key = b.order_id
  JOIN r_month rm
    ON rm.courier_id = b.courier_id
   AND rm.y = b.y
   AND rm.m = b.m
),
m AS (
  SELECT
      courier_id,
      MAX(courier_name) AS courier_name,
      settlement_year,
      settlement_month,
      COUNT(DISTINCT order_id) AS orders_count,
      ROUND(SUM(order_total), 2)           AS orders_total_sum,
      AVG(r)::float8                       AS rate_avg,
      ROUND(SUM(tip_sum), 2)               AS courier_tips_sum,
      ROUND(SUM(GREATEST(pct * order_total, min_per_order)), 2) AS courier_order_sum
  FROM per_order
  GROUP BY courier_id, settlement_year, settlement_month
)
INSERT INTO cdm.dm_courier_ledger (
  courier_id, courier_name, settlement_year, settlement_month,
  orders_count, orders_total_sum, rate_avg,
  order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum
)
SELECT
  courier_id,
  courier_name,
  settlement_year,
  settlement_month,
  orders_count,
  orders_total_sum,
  rate_avg,
  ROUND(orders_total_sum * 0.25, 2) AS order_processing_fee,
  courier_order_sum,
  courier_tips_sum,
  ROUND(courier_order_sum + courier_tips_sum * 0.95, 2) AS courier_reward_sum
FROM m
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET courier_name         = EXCLUDED.courier_name,
    orders_count         = EXCLUDED.orders_count,
    orders_total_sum     = EXCLUDED.orders_total_sum,
    rate_avg             = EXCLUDED.rate_avg,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_order_sum    = EXCLUDED.courier_order_sum,
    courier_tips_sum     = EXCLUDED.courier_tips_sum,
    courier_reward_sum   = EXCLUDED.courier_reward_sum;
"""


class CourierMonthlyMartLoader:
    """Пересчитывает помесячную витрину курьеров."""
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.log = log

    def load(self) -> None:
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(UPSERT_SQL)
        self.log.info("cdm.dm_courier_ledger — updated.")
