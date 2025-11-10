from logging import Logger
from typing import List, Dict

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import dict_row


class UserObj:
    def __init__(self, object_id: str, login: str, name: str, update_ts: str) -> None:
        self.object_id = object_id
        self.login = login
        self.name = name
        self.update_ts = update_ts


class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, threshold_ts: str, limit: int) -> List[UserObj]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT object_id, object_value, update_ts
                    FROM stg.ordersystem_users
                    WHERE update_ts > %(threshold)s
                    ORDER BY update_ts ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold_ts, "limit": limit}
            )
            rows = cur.fetchall()

        result: List[UserObj] = []
        for r in rows:
            obj: Dict = str2json(r["object_value"])
            result.append(
                UserObj(
                    object_id=r["object_id"],
                    login=obj["login"],
                    name=obj["name"],
                    update_ts=r["update_ts"]
                )
            )
        return result


class UsersDestRepository:
    def insert_user(self, conn: Connection, user: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_login, user_name)
                    VALUES (%(user_id)s, %(user_login)s, %(user_name)s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET user_login = EXCLUDED.user_login,
                        user_name  = EXCLUDED.user_name;
                """,
                {
                    "user_id": user.object_id,
                    "user_login": user.login,
                    "user_name": user.name,
                },
            )


class UserLoader:
    WF_KEY = "stg_ordersystem_users_to_dds_dm_users"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = UsersOriginRepository(pg_origin)
        self.dds = UsersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_users(self):
        self.log.info("Opening DWH connection...")
        with self.pg_dest.connection() as conn:
            self.log.info("DWH connection opened. Loading ETL setting...")

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "1900-01-01 00:00:00"}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.dds.insert_user(conn, user)

            # фиксируем прогресс
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([u.update_ts for u in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")