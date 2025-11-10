from logging import Logger
from typing import List, Dict
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import dict_row



class RestaurantObj:
    def __init__(self, restaurant_id: str, name: str, update_ts: datetime):
        self.restaurant_id = restaurant_id
        self.name = name
        self.update_ts = update_ts

class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, threshold_ts: str, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %(threshold)s
                    ORDER BY update_ts ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold_ts, "limit": limit}
            )
            rows = cur.fetchall()

        result: List[RestaurantObj] = []
        for r in rows:
            obj: Dict = str2json(r["object_value"])
            result.append(
                RestaurantObj(
                    restaurant_id=r["object_id"],
                    name=obj["name"],
                    update_ts=r["update_ts"]
                )
            )
        return result
    
class RestaurantsDestRepository:
    def insert_restaurant(self, conn: Connection, rest: RestaurantObj) -> None:
        with conn.cursor() as cur:
            # Закрыть старую запись, если она ещё актуальна
            cur.execute(
                """
                UPDATE dds.dm_restaurants
                SET active_to = %(update_ts)s
                WHERE restaurant_id = %(restaurant_id)s
                  AND active_to = '2099-12-31 00:00:00';
                """,
                {
                    "restaurant_id": rest.restaurant_id,
                    "update_ts": rest.update_ts
                }
            )

            # Вставить новую версию
            cur.execute(
                """
                INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, '2099-12-31 00:00:00')
                ON CONFLICT (restaurant_id, active_from) DO NOTHING;
                """,
                {
                    "restaurant_id": rest.restaurant_id,
                    "restaurant_name": rest.name,
                    "active_from": rest.update_ts
                }
            )


class RestaurantLoader:
    WF_KEY = "stg_ordersystem_restaurants_to_dds_dm_restaurants"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestaurantsOriginRepository(pg_origin)
        self.dds = RestaurantsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "1900-01-01 00:00:00"}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)

            if not load_queue:
                self.log.info("No new restaurants.")
                return

            for rest in load_queue:
                self.dds.insert_restaurant(conn, rest)

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([r.update_ts for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Loaded {len(load_queue)} restaurants, last_ts={wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")

