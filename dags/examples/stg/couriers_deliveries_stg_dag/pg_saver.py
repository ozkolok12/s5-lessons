from typing import List, Dict
from lib import PgConnect
from lib.dict_util import json2str

class PgSaver:
    def __init__(self, pg: PgConnect):
        self._pg = pg

    def save_couriers(self, rows: List[Dict]):
        if not rows:
            return
        with self._pg.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO stg.ordersystem_couriers (object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s::jsonb, NOW())
                    ON CONFLICT (object_id) DO NOTHING;
                    """,
                    [{"object_id": r["_id"], "object_value": json2str(r)} for r in rows]
                )

    def save_deliveries(self, rows: List[Dict]):
        if not rows:
            return
        with self._pg.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO stg.ordersystem_deliveries (object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s::jsonb, NOW())
                    ON CONFLICT (object_id) DO NOTHING;
                    """,
                    [{"object_id": r["order_id"], "object_value": json2str(r)} for r in rows]
                )