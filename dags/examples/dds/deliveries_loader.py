# dds_deliveries_loader.py
from logging import Logger
from typing import List, Tuple, Optional
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg.rows import dict_row
from psycopg import Connection


class DeliveryRow:
    def __init__(
        self,
        order_id: str,
        courier_id: Optional[str],
        order_ts: Optional[datetime],
        rate: Optional[float],
        tip_sum: Optional[float],
    ):
        self.order_id = order_id
        self.courier_id = courier_id or ""
        self.order_ts = order_ts
        self.rate = rate
        self.tip_sum = tip_sum


class DeliveriesOriginRepository:
    """Инкрементально читает доставки из stg.ordersystem_deliveries по update_ts."""
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_rows(self, threshold_ts: str, limit: int) -> List[Tuple[DeliveryRow, datetime]]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT object_value, update_ts
                  FROM stg.ordersystem_deliveries
                 WHERE update_ts > %(threshold)s
                 ORDER BY update_ts ASC
                 LIMIT %(limit)s;
                """,
                {"threshold": threshold_ts, "limit": limit},
            )
            rows = cur.fetchall()

        result: List[Tuple[DeliveryRow, datetime]] = []
        for r in rows:
            val = r["object_value"]
            obj = val if isinstance(val, dict) else str2json(val)
            upd = r["update_ts"]

            order_id = obj.get("order_id")
            if not order_id:
                continue

            courier_id = obj.get("courier_id")

            ots_raw = obj.get("order_ts")
            try:
                order_ts = datetime.fromisoformat(ots_raw) if ots_raw else None
            except Exception:
                order_ts = None

            def _to_float(x):
                try:
                    return float(x) if x is not None else None
                except Exception:
                    return None

            rate = _to_float(obj.get("rate"))
            tip_sum = _to_float(obj.get("tip_sum"))

            result.append((DeliveryRow(order_id, courier_id, order_ts, rate, tip_sum), upd))

        return result


class DeliveriesDestRepository:
    """Upsert  в dds.fct_deliveries."""
    def upsert_row(self, conn: Connection, d: DeliveryRow) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.fct_deliveries (
                    order_id, courier_id, order_ts, rate, tip_sum
                )
                VALUES (%(oid)s, %(cid)s, %(ots)s, %(rate)s, %(tips)s)
                ON CONFLICT (order_id) DO UPDATE
                SET courier_id = EXCLUDED.courier_id,
                    order_ts   = EXCLUDED.order_ts,
                    rate       = EXCLUDED.rate,
                    tip_sum    = EXCLUDED.tip_sum;
                """,
                {
                    "oid": d.order_id,
                    "cid": d.courier_id,
                    "ots": d.order_ts,
                    "rate": d.rate,
                    "tips": d.tip_sum,
                },
            )

class DeliveriesLoader:
    WF_KEY = "stg_ordersystem_deliveries_to_dds_fct_deliveries"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.dest = DeliveriesDestRepository()
        self.settings_repo = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self) -> None:
        with self.pg_dest.connection() as conn:
            wf = self.settings_repo.get_setting(conn, self.WF_KEY)
            if not wf:
                wf = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "1900-01-01 00:00:00"},
                )

            last_loaded = wf.workflow_settings[self.LAST_LOADED_TS_KEY]
            processed = 0

            while True:
                batch = self.origin.list_rows(last_loaded, self.BATCH_LIMIT)
                if not batch:
                    break

                for row, _upd in batch:
                    self.dest.upsert_row(conn, row)

                processed += len(batch)
                last_loaded = max(
                    (u if isinstance(u, datetime) else datetime.fromisoformat(u)) for _, u in batch
                ).isoformat(" ")
                wf.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded
                self.settings_repo.save_setting(conn, wf.workflow_key, json2str(wf.workflow_settings))

            self.log.info(f"Loaded {processed} deliveries, last_update_ts={last_loaded}")
