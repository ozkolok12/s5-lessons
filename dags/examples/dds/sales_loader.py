# sales_loader.py
from logging import Logger
from typing import List, Dict, Tuple
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import dict_row


class SaleObj:
    def __init__(
        self,
        order_key: str,
        ts: datetime,
        product_src_id: str,
        restaurant_src_id: str,
        count: int,
        price: float,
        bonus_payment: float,
        bonus_grant: float,
    ):
        self.order_key = order_key
        self.ts = ts
        self.product_src_id = product_src_id
        self.restaurant_src_id = restaurant_src_id
        self.count = count
        self.price = price
        self.bonus_payment = bonus_payment
        self.bonus_grant = bonus_grant

    @property
    def total_sum(self) -> float:
        return self.count * self.price


# ─────────────────────────── SRC ────────────────────────────
class SalesOriginRepository:
    """
    • Берём финальные заказы из stg.ordersystem_orders.
    • Для watermark берём batch, вытаскиваем order_items.
    • К каждому (order_key, product_id) подтягиваем бонусы из stg.bonussystem_events:
        - суммируем event_value.bonus_payment в BONUS_PAYMENT-событиях,
        - суммируем event_value.bonus_grant   в BONUS_GRANT-событиях.
    """
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def _bonus_maps(self, order_keys: List[str]) -> Tuple[Dict[Tuple[str, str], float], Dict[Tuple[str, str], float]]:
        if not order_keys:
            return {}, {}

        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    (event_value::jsonb->>'order_id')   AS order_key,
                    (jsonb_array_elements(event_value::jsonb->'product_payments')->>'product_id')   AS product_id,
                    (jsonb_array_elements(event_value::jsonb->'product_payments')->>'bonus_payment')::numeric AS bonus_payment,
                    (jsonb_array_elements(event_value::jsonb->'product_payments')->>'bonus_grant')::numeric   AS bonus_grant
                FROM stg.bonussystem_events
                WHERE event_type = 'bonus_transaction'
                AND (event_value::jsonb->>'order_id') = ANY(%(keys)s);
                """,
                {"keys": order_keys},
            )
            rows = cur.fetchall()

        pay_map: Dict[Tuple[str, str], float] = {}
        grant_map: Dict[Tuple[str, str], float] = {}

        for r in rows:
            k = (r["order_key"], r["product_id"])
            pay_map[k] = float(r["bonus_payment"] or 0)
            grant_map[k] = float(r["bonus_grant"] or 0)

        return pay_map, grant_map

    def list_sales(self, threshold_ts: str, limit: int) -> List[Tuple[SaleObj, datetime]]:
        """Возвращаем список (SaleObj, update_ts) инкрементально по update_ts."""
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT object_value, update_ts
                  FROM stg.ordersystem_orders
                    WHERE update_ts > %(threshold)s
                    AND (object_value::jsonb->>'final_status') = 'CLOSED'
                 ORDER BY update_ts ASC
                 LIMIT %(limit)s;
                """,
                {"threshold": threshold_ts, "limit": limit},
            )
            rows = cur.fetchall()

        order_keys: List[str] = []
        parsed_orders: List[Tuple[Dict, datetime]] = []
        for r in rows:
            obj = str2json(r["object_value"])
            upd_ts = r["update_ts"]
            ok = obj.get("_id")
            if ok:
                order_keys.append(ok)
                parsed_orders.append((obj, upd_ts))

        pay_map, grant_map = self._bonus_maps(order_keys)

        # собираем SaleObj
        result: List[Tuple[SaleObj, datetime]] = []
        for obj, upd_ts in parsed_orders:
            order_key = obj.get("_id")
            date_str = obj.get("date")
            ts = datetime.fromisoformat(date_str) if date_str else None
            rest_id = (obj.get("restaurant") or {}).get("id")
            if not (order_key and ts and rest_id):
                continue

            for it in obj.get("order_items", []) or []:
                prod_id = it.get("id")
                if not prod_id:
                    continue

                sale = SaleObj(
                    order_key=order_key,
                    ts=ts,
                    product_src_id=prod_id,
                    restaurant_src_id=rest_id,
                    count=int(it.get("quantity", 0)),
                    price=float(it.get("price", 0)),
                    bonus_payment=pay_map.get((order_key, prod_id), 0.0),
                    bonus_grant=grant_map.get((order_key, prod_id), 0.0),
                )
                result.append((sale, upd_ts))

        return result


# ─────────────────────────── DST ────────────────────────────
class SalesDestRepository:
    def insert_sale(self, conn: Connection, sale: SaleObj, log: Logger) -> None:
        with conn.cursor() as cur:
            # 1) order_id
            cur.execute(
                "SELECT id FROM dds.dm_orders WHERE order_key = %(ok)s LIMIT 1;",
                {"ok": sale.order_key},
            )
            r = cur.fetchone()
            if not r:
                log.info(f"skip sale: order {sale.order_key} not found")
                return
            order_pk: int = r[0]

            # 2) product_id — сначала версия нужного ресторана, затем fallback
            cur.execute(
                """
                SELECT dp.id
                  FROM dds.dm_products dp
                  JOIN dds.dm_restaurants dr ON dr.id = dp.restaurant_id
                 WHERE dp.product_id    = %(prod_src)s
                   AND dr.restaurant_id = %(rest_src)s
                 ORDER BY (dp.active_to = '2099-12-31') DESC, dp.active_from DESC
                 LIMIT 1;
                """,
                {"prod_src": sale.product_src_id, "rest_src": sale.restaurant_src_id},
            )
            r = cur.fetchone()
            if not r:
                cur.execute(
                    """
                    SELECT id
                      FROM dds.dm_products
                     WHERE product_id = %(prod_src)s
                     ORDER BY (active_to = '2099-12-31') DESC, active_from DESC
                     LIMIT 1;
                    """,
                    {"prod_src": sale.product_src_id},
                )
                r = cur.fetchone()
                if not r:
                    log.info(f"skip sale ({sale.order_key}): product {sale.product_src_id} not found")
                    return
            product_pk: int = r[0]

            # 3) upsert факта
            cur.execute(
                """
                INSERT INTO dds.fct_product_sales(
                    product_id, order_id,
                    count, price, total_sum,
                    bonus_payment, bonus_grant
                )
                VALUES (
                    %(product_id)s, %(order_id)s,
                    %(cnt)s, %(price)s, %(total)s,
                    %(bonus_pay)s, %(bonus_gr)s
                )
                ON CONFLICT (product_id, order_id) DO UPDATE
                SET count         = EXCLUDED.count,
                    price         = EXCLUDED.price,
                    total_sum     = EXCLUDED.total_sum,
                    bonus_payment = EXCLUDED.bonus_payment,
                    bonus_grant   = EXCLUDED.bonus_grant;
                """,
                {
                    "product_id": product_pk,
                    "order_id":   order_pk,
                    "cnt":        sale.count,
                    "price":      sale.price,
                    "total":      sale.total_sum,
                    "bonus_pay":  sale.bonus_payment,
                    "bonus_gr":   sale.bonus_grant,
                },
            )


# ───────────────────────── LOADER ───────────────────────────
class SalesLoader:
    WF_KEY = "stg_ordersystem_orders_to_dds_fct_product_sales"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SalesOriginRepository(pg_origin)
        self.dds = SalesDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_sales(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "1900-01-01 00:00:00"},
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            processed = 0

            while True:
                batch = self.origin.list_sales(last_loaded, self.BATCH_LIMIT)
                if not batch:
                    break

                for sale_obj, _upd in batch:
                    try:
                        self.dds.insert_sale(conn, sale_obj, self.log)
                    except Exception as e:
                        self.log.exception(
                            f"failed sale (order={sale_obj.order_key}, "
                            f"product={sale_obj.product_src_id}): {e}"
                        )
                processed += len(batch)

                # watermark
                last_loaded = max(
                    (u if isinstance(u, datetime) else datetime.fromisoformat(u)) for _, u in batch
                ).isoformat(" ")
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, json2str(wf_setting.workflow_settings)
                )

            self.log.info(f"Loaded {processed} product-sales rows, last_update_ts={last_loaded}")