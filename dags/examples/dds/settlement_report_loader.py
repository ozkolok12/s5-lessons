# dags/cdm_dm_settlement_report.py
from __future__ import annotations

from datetime import datetime, date, timedelta
from logging import Logger, getLogger

from airflow import DAG
from airflow.operators.python import PythonOperator

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import dict_row

# --- SQL: дневной расчёт с UTC-датой и upsert --------------------------------
SQL_DM_SETTLEMENT_REPORT = """
WITH order_level AS (
    SELECT
        fps.order_id,
        MAX(fps.total_sum)     AS order_total_sum,
        MAX(fps.bonus_payment) AS order_bonus_payment_sum,
        MAX(fps.bonus_grant)   AS order_bonus_granted_sum
    FROM dds.fct_product_sales fps
    GROUP BY fps.order_id
),
closed_orders AS (
    SELECT
        o.id            AS order_id,
        o.restaurant_id,                   -- строковый ID в исходных данных
        dt.ts,                             -- точный момент заказа (timestamp/timestamptz)
        (dt.ts AT TIME ZONE 'UTC')::date AS order_date_utc,  -- ключ: дата в UTC
        ol.order_total_sum,
        ol.order_bonus_payment_sum,
        ol.order_bonus_granted_sum
    FROM dds.dm_orders o
    JOIN order_level       ol ON ol.order_id = o.id
    JOIN dds.dm_timestamps dt ON dt.id       = o.timestamp_id
    WHERE o.order_status = 'CLOSED'
),
with_restaurant_name AS (
    SELECT
        co.restaurant_id::varchar AS restaurant_id,
        dr.restaurant_name,
        co.order_date_utc         AS settlement_date,  -- считаем по ДНЯМ (UTC)
        co.order_id,
        co.order_total_sum,
        co.order_bonus_payment_sum,
        co.order_bonus_granted_sum
    FROM closed_orders co
    JOIN dds.dm_restaurants dr
      ON dr.restaurant_id = co.restaurant_id::varchar
     AND co.ts >= dr.active_from
     AND (dr.active_to IS NULL OR co.ts < dr.active_to)
),
daily AS (
    SELECT
        restaurant_id,
        MAX(restaurant_name)                     AS restaurant_name,
        settlement_date,
        COUNT(DISTINCT order_id)                 AS orders_count,
        SUM(order_total_sum)                     AS orders_total_sum,
        SUM(order_bonus_payment_sum)             AS orders_bonus_payment_sum,
        SUM(order_bonus_granted_sum)             AS orders_bonus_granted_sum
    FROM with_restaurant_name
    WHERE settlement_date BETWEEN %(from_date)s AND %(to_date)s
    GROUP BY restaurant_id, settlement_date
)
INSERT INTO cdm.dm_settlement_report AS r (
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
    d.restaurant_id,
    d.restaurant_name,
    d.settlement_date,
    d.orders_count,
    d.orders_total_sum,
    d.orders_bonus_payment_sum,
    d.orders_bonus_granted_sum,
    (d.orders_total_sum * 0.25) AS order_processing_fee,
    (d.orders_total_sum - d.orders_bonus_payment_sum - (d.orders_total_sum * 0.25)) AS restaurant_reward_sum
FROM daily d
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET restaurant_name            = EXCLUDED.restaurant_name,
    orders_count               = EXCLUDED.orders_count,
    orders_total_sum           = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum   = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum   = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee       = EXCLUDED.order_processing_fee,
    restaurant_reward_sum      = EXCLUDED.restaurant_reward_sum;
"""

# --- Loader -------------------------------------------------------------------
class SettlementReportLoader:
    WF_KEY = "dds_to_cdm_dm_settlement_report"
    SAFETY_DAYS_BACK = 1  # пересчитываем хвост на 1 день назад (на случай поздних событий)

    def __init__(self, pg_dest: PgConnect, log: Logger):
        self.pg = pg_dest
        self.log = log
        self.settings = StgEtlSettingsRepository()

    def _ensure_index(self, conn: Connection):
        """Индекс под upsert. restaurant_id должен быть VARCHAR в CDM!"""
        with conn.cursor() as cur:
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_settlement_report_rest_date
                ON cdm.dm_settlement_report(restaurant_id, settlement_date);
            """)

    def _detect_window(self, conn: Connection, last_loaded_date: date) -> tuple[date, date] | None:
        """Окно ДАТ: от min(DATE(dt.ts)) до вчера (UTC-ориентир в проверке),
        начиная с (last_loaded_date - SAFETY_DAYS_BACK)."""
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    MIN(DATE(dt.ts)) AS min_date,
                    MAX(DATE(dt.ts)) AS max_date
                FROM dds.dm_orders o
                JOIN dds.dm_timestamps dt ON dt.id = o.timestamp_id
                WHERE o.order_status = 'CLOSED'
                  AND DATE(dt.ts) >= %(from_date)s
                """,
                {"from_date": last_loaded_date - timedelta(days=self.SAFETY_DAYS_BACK)}
            )
            row = cur.fetchone()

        if not row or row["min_date"] is None:
            return None

        min_date = row["min_date"]
        to_date  = date.today() - timedelta(days=1)  # до ВЧЕРА, чтобы совпасть с проверкой
        if min_date > to_date:
            return None
        return (min_date, to_date)

    def _upsert_days(self, conn: Connection, from_date: date, to_date: date):
        with conn.cursor() as cur:
            cur.execute(SQL_DM_SETTLEMENT_REPORT, {
                "from_date": from_date,
                "to_date": to_date
            })

    def load(self):
        with self.pg.connection() as conn:
            self._ensure_index(conn)

            wf = self.settings.get_setting(conn, self.WF_KEY)
            if not wf:
                wf = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={"last_loaded_date": "1900-01-01"}
                )

            last_loaded_date = datetime.strptime(
                wf.workflow_settings["last_loaded_date"], "%Y-%m-%d"
            ).date()

            win = self._detect_window(conn, last_loaded_date)
            if not win:
                self.log.info("Новых дат нет — выходим.")
                return

            from_date, to_date = win
            self.log.info(f"Пересчитываю дни с {from_date} по {to_date} включительно...")
            self._upsert_days(conn, from_date, to_date)

            # чекпоинт = последний пересчитанный день
            wf.workflow_settings["last_loaded_date"] = to_date.isoformat()
            self.settings.save_setting(conn, wf.workflow_key, json2str(wf.workflow_settings))
            self.log.info(f"Готово. last_loaded_date = {wf.workflow_settings['last_loaded_date']}")