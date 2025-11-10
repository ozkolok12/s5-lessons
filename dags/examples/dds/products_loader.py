from logging import Logger
from typing import List, Dict, Optional, Tuple
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import dict_row


class ProductObj:
    def __init__(self,
                 product_id: str,
                 product_name: str,
                 product_price: float,
                 active_from: datetime,
                 restaurant_id: str):
        self.product_id = product_id
        self.product_name = product_name
        self.product_price = product_price
        self.active_from = active_from
        self.restaurant_id = restaurant_id


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, threshold_ts: str, limit: int) -> List[Tuple[ProductObj, datetime]]:
        """
        Возвращает список кортежей (ProductObj, update_ts) инкрементально по update_ts.
        Берём только финальные заказы и в пределах батча дедупим по (product_id, restaurant_id),
        чтобы не плодить дубликаты из каждого заказа.
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

        result: List[Tuple[ProductObj, datetime]] = []
        seen = set()  # (product_id, restaurant_id)

        for r in rows:
            obj: Dict = str2json(r["object_value"])
            update_ts = r["update_ts"]

            rest_id = (obj.get("restaurant") or {}).get("id")
            if not rest_id:
                continue

            order_items = obj.get("order_items", []) or []
            for item in order_items:
                pid = item.get("id")
                if not pid:
                    continue
                key = (pid, rest_id)
                if key in seen:
                    continue
                seen.add(key)

                name = item.get("name") or ""
                price = item.get("price")
                try:
                    price_val = float(price) if price is not None else 0.0
                except Exception:
                    price_val = 0.0

                prod = ProductObj(
                    product_id=pid,
                    product_name=name,
                    product_price=price_val,
                    active_from=update_ts,
                    restaurant_id=rest_id
                )
                result.append((prod, update_ts))

        return result


class ProductsDestRepository:
    def insert_product(self, conn: Connection, prod: ProductObj, log: Logger) -> None:
        """
        1) Находим PK актуальной версии ресторана (active_to = '2099-12-31').
        2) INSERT ... ON CONFLICT (product_id, restaurant_id) DO UPDATE:
           - обновляем name/price,
           - active_from сохраняем минимальный из (существующего, нового),
           - active_to фиксируем как '2099-12-31'.
        """
        with conn.cursor() as cur:
            # 1) ресторан (актуальная версия)
            cur.execute(
                """
                SELECT id
                  FROM dds.dm_restaurants
                 WHERE restaurant_id = %(rest_src)s
                   AND active_to = '2099-12-31'
                 LIMIT 1;
                """,
                {"rest_src": prod.restaurant_id}
            )
            row = cur.fetchone()
            if not row:
                log.info(f"skip product {prod.product_id}: no active restaurant for source id {prod.restaurant_id}")
                return

            rest_pk = row[0]

            # 2) upsert одной записи на (product_id, restaurant_id)
            cur.execute(
                """
                INSERT INTO dds.dm_products(
                    product_id, product_name, product_price,
                    active_from, active_to, restaurant_id
                )
                VALUES (
                    %(product_id)s, %(product_name)s, %(product_price)s,
                    %(active_from)s, '2099-12-31', %(restaurant_pk)s
                )
                ON CONFLICT (product_id, restaurant_id)
                DO UPDATE
                SET product_name = EXCLUDED.product_name,
                    product_price = EXCLUDED.product_price,
                    active_from  = LEAST(dds.dm_products.active_from, EXCLUDED.active_from),
                    active_to    = '2099-12-31';
                """,
                {
                    "product_id": prod.product_id,
                    "product_name": prod.product_name,
                    "product_price": prod.product_price,
                    "active_from": prod.active_from,
                    "restaurant_pk": rest_pk
                }
            )


class ProductsLoader:
    WF_KEY = "stg_ordersystem_orders_to_dds_dm_products"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.dds = ProductsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
                load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
                if not load_queue:
                    break

                for prod_obj, _upd in load_queue:
                    try:
                        self.dds.insert_product(conn, prod_obj, self.log)
                    except Exception as e:
                        self.log.exception(f"failed to upsert product {prod_obj.product_id}: {e}")
                total_processed += len(load_queue)

                # водяной знак по max(update_ts) из батча
                max_update_ts = max(
                    (u if isinstance(u, datetime) else datetime.fromisoformat(u))
                    for _, u in load_queue
                )
                last_loaded = max_update_ts.isoformat(sep=" ")

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            if total_processed == 0:
                self.log.info("No new products.")
            else:
                self.log.info(f"Loaded {total_processed} unique products (in-batch), last_update_ts={last_loaded}")