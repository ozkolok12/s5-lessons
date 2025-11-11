# dds_couriers_loader.py
from logging import Logger
from typing import List, Tuple, Optional
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg.rows import dict_row
from psycopg import Connection


class CourierObj:
    def __init__(self, courier_id: str, courier_name: Optional[str]):
        self.courier_id = courier_id
        self.courier_name = courier_name or ""


class CouriersOriginRepository:
    """Читает  курьеров из stg.ordersystem_couriers инкрементально по update_ts."""
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, threshold_ts: str, limit: int) -> List[Tuple[CourierObj, datetime]]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT object_value, update_ts
                  FROM stg.ordersystem_couriers
                 WHERE update_ts > %(threshold)s
                 ORDER BY update_ts ASC
                 LIMIT %(limit)s;
                """,
                {"threshold": threshold_ts, "limit": limit},
            )
            rows = cur.fetchall()

        result: List[Tuple[CourierObj, datetime]] = []
        for r in rows:
            val = r["object_value"]
            obj = val if isinstance(val, dict) else str2json(val)
            upd = r["update_ts"]

            # ожидаем: { "_id": "...", "name": "..." }
            cid = (obj or {}).get("_id")
            cname = (obj or {}).get("name") or ((obj or {}).get("courier") or {}).get("name")

            if not cid:
                # пропускаем кривые записи без идентификатора
                continue

            result.append((CourierObj(courier_id=cid, courier_name=cname), upd))

        return result


class CouriersDestRepository:
    """Upsert в dds.dm_couriers по натуральному ключу courier_id."""
    def upsert_courier(self, conn: Connection, c: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_couriers (courier_id, courier_name)
                VALUES (%(cid)s, %(cname)s)
                ON CONFLICT (courier_id) DO UPDATE
                SET courier_name = EXCLUDED.courier_name;
                """,
                {"cid": c.courier_id, "cname": c.courier_name},
            )


class CouriersLoader:
    """
    Инкрементальная загрузка измерения dds.dm_couriers из stg.ordersystem_couriers.
    Водяной знак: last_loaded_ts по полю update_ts в STG.
    """
    WF_KEY = "stg_ordersystem_couriers_to_dds_dm_couriers"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginRepository(pg_origin)
        self.dest = CouriersDestRepository()
        self.settings_repo = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self) -> None:
        with self.pg_dest.connection() as conn:
            # читаем/инициализируем watermark
            wf_setting = self.settings_repo.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "1900-01-01 00:00:00"},
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            total = 0

            while True:
                batch = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT)
                if not batch:
                    break

                for cour_obj, _upd in batch:
                    self.dest.upsert_courier(conn, cour_obj)

                total += len(batch)

                # обновляем watermark по максимальному update_ts в пачке
                last_loaded = max(
                    (u if isinstance(u, datetime) else datetime.fromisoformat(u)) for _, u in batch
                ).isoformat(" ")
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded
                self.settings_repo.save_setting(
                    conn, wf_setting.workflow_key, json2str(wf_setting.workflow_settings)
                )

            self.log.info(f"Loaded {total} couriers, last_update_ts={last_loaded}")
