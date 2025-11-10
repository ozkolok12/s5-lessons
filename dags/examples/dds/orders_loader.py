from logging import Logger
from typing import List, Dict, Tuple, Optional
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import dict_row


class OrderObj:
    def __init__(self,
                 order_key: str,
                 order_status: str,
                 ts: datetime,
                 restaurant_id: str,
                 user_id: str):
        self.order_key = order_key
        self.order_status = order_status
        self.ts = ts
        self.restaurant_id = restaurant_id
        self.user_id = user_id


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, threshold_ts: str, limit: int) -> List[Tuple[OrderObj, datetime]]:
        """
        Возвращает список кортежей (OrderObj, update_ts) по заказам.
        Берём только финальные статусы.
        """
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT object_value, update_ts
                  FROM stg.ordersystem_orders
                 WHERE update_ts > %(threshold)s
                   AND (object_value::jsonb->>'final_status') IN ('CLOSED','CANCELLED','CANCELED')
                 ORDER BY update_ts ASC
                 LIMIT %(limit)s;
                """,
                {"threshold": threshold_ts, "limit": limit}
            )
            rows = cur.fetchall()

        result: List[Tuple[OrderObj, datetime]] = []
        for r in rows:
            obj: Dict = str2json(r["object_value"])
            update_ts = r["update_ts"]

            order_id = obj.get("_id")
            status = obj.get("final_status")
            date_str = obj.get("date")
            ts = datetime.fromisoformat(date_str) if date_str else None
            rest_id = (obj.get("restaurant") or {}).get("id")
            user_id = (obj.get("user") or {}).get("id")

            if not (order_id and ts and rest_id and user_id):
                continue

            result.append((OrderObj(order_id, status, ts, rest_id, user_id), update_ts))

        return result


class OrdersDestRepository:
    def insert_order(self, conn: Connection, order: OrderObj, log: Logger) -> None:
        """
        Вставка заказа с разрешением FK:
        - ищем restaurant_id в dm_restaurants (активная версия на момент ts),
        - ищем user_id в dm_users,
        - ищем timestamp_id в dm_timestamps.
        """
        with conn.cursor() as cur:
            # timestamp
            cur.execute(
                """
                SELECT id FROM dds.dm_timestamps
                WHERE ts = %(ts)s
                LIMIT 1;
                """,
                {"ts": order.ts}
            )
            ts_row = cur.fetchone()
            if not ts_row:
                log.info(f"skip order {order.order_key}: no timestamp {order.ts}")
                return
            ts_pk = ts_row[0]

            # restaurant
            cur.execute(
                """
                SELECT id
                  FROM dds.dm_restaurants
                 WHERE restaurant_id = %(rest_src)s
                   AND %(ts)s >= active_from
                   AND %(ts)s < active_to
                 LIMIT 1;
                """,
                {"rest_src": order.restaurant_id, "ts": order.ts}
            )
            rest_row = cur.fetchone()
            if not rest_row:
                log.info(f"skip order {order.order_key}: no restaurant {order.restaurant_id} at {order.ts}")
                return
            rest_pk = rest_row[0]

            # user
            cur.execute(
                """
                SELECT id FROM dds.dm_users
                WHERE user_id = %(user_src)s
                LIMIT 1;
                """,
                {"user_src": order.user_id}
            )
            user_row = cur.fetchone()
            if not user_row:
                log.info(f"skip order {order.order_key}: no user {order.user_id}")
                return
            user_pk = user_row[0]

            # upsert в dm_orders
            cur.execute(
                """
                INSERT INTO dds.dm_orders(order_key, order_status,
                                          restaurant_id, timestamp_id, user_id)
                VALUES (%(order_key)s, %(order_status)s,
                        %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                ON CONFLICT (order_key) DO UPDATE
                SET order_status = EXCLUDED.order_status,
                    restaurant_id = EXCLUDED.restaurant_id,
                    timestamp_id = EXCLUDED.timestamp_id,
                    user_id = EXCLUDED.user_id;
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "restaurant_id": rest_pk,
                    "timestamp_id": ts_pk,
                    "user_id": user_pk
                }
            )


class OrdersLoader:
    WF_KEY = "stg_ordersystem_orders_to_dds_dm_orders"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.dds = OrdersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "1900-01-01 00:00:00"}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            total_processed = 0

            while True:
                load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
                if not load_queue:
                    break

                for order_obj, _upd in load_queue:
                    try:
                        self.dds.insert_order(conn, order_obj, self.log)
                    except Exception as e:
                        self.log.exception(f"failed to upsert order {order_obj.order_key}: {e}")
                total_processed += len(load_queue)

                # обновляем watermark
                max_update_ts = max(
                    (u if isinstance(u, datetime) else datetime.fromisoformat(u))
                    for _, u in load_queue
                )
                last_loaded = max_update_ts.isoformat(sep=" ")

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            if total_processed == 0:
                self.log.info("No new orders.")
            else:
                self.log.info(f"Loaded {total_processed} orders, last_update_ts={last_loaded}")
