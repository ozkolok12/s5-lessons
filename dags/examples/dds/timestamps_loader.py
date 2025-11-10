from logging import Logger
from typing import List, Dict, Optional, Tuple
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import dict_row


class TimestampObj:
    def __init__(self, ts: datetime):
        self.ts = ts
        self.year = ts.year
        self.month = ts.month
        self.day = ts.day
        self.date = ts.date()
        self.time = ts.time()


class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, threshold_ts: str, limit: int) -> List[Tuple[TimestampObj, datetime]]:
        """
        Возвращает список кортежей (TimestampObj, update_ts).
        Фильтрация по update_ts > threshold_ts для инкремента.
        """
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT object_value, update_ts
                  FROM stg.ordersystem_orders
                 WHERE update_ts > %(threshold)s
                 ORDER BY update_ts ASC
                 LIMIT %(limit)s;
                """,
                {"threshold": threshold_ts, "limit": limit}
            )
            rows = cur.fetchall()

        result: List[Tuple[TimestampObj, datetime]] = []

        for r in rows:
            obj: Dict = str2json(r["object_value"])
            update_ts = r["update_ts"]  # datetime (обычно), но на всякий случай можно парсить строку
            final_status = (obj.get("final_status") or "").upper()

            # интересуют только финальные состояния
            if final_status not in ("CLOSED", "CANCELLED", "CANCELED"):
                continue

            # основная дата — поле "date" (по заданию)
            date_str: Optional[str] = obj.get("date")

            ts: Optional[datetime] = None
            if date_str:
                # "2025-08-13 09:40:52" — ok для fromisoformat
                ts = datetime.fromisoformat(date_str)
            else:
                # запасной путь — найти dttm соответствующего финального статуса в списке статусов
                statuses = obj.get("statuses", []) or []
                target_status = "CANCELLED" if final_status in ("CANCELLED", "CANCELED") else "CLOSED"
                for st in statuses:
                    if (st.get("status") or "").upper() == target_status:
                        ts = datetime.fromisoformat(st["dttm"])
                        break

            if ts is None:
                # если не смогли извлечь дату — пропускаем запись
                continue

            result.append((TimestampObj(ts), update_ts))

        return result


class TimestampsDestRepository:
    def insert_timestamp(self, conn: Connection, ts: TimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                ON CONFLICT (ts) DO UPDATE
                SET year=EXCLUDED.year, month=EXCLUDED.month, day=EXCLUDED.day,
                    date=EXCLUDED.date, time=EXCLUDED.time;
                """,
                {
                    "ts": ts.ts,
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "date": ts.date,
                    "time": ts.time
                }
            )


class TimestampLoader:
    WF_KEY = "stg_ordersystem_orders_to_dds_dm_timestamps"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampsOriginRepository(pg_origin)
        self.dds = TimestampsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "1900-01-01 00:00:00"}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]

            total_inserted = 0
            while True:
                load_queue = self.origin.list_timestamps(last_loaded, self.BATCH_LIMIT)
                if not load_queue:
                    break

                # вставляем в dm_timestamps
                for ts_obj, _upd in load_queue:
                    self.dds.insert_timestamp(conn, ts_obj)
                total_inserted += len(load_queue)

                # водяной знак ведём по МАКСИМАЛЬНОМУ update_ts из батча
                max_update_ts = max(
                    (u if isinstance(u, datetime) else datetime.fromisoformat(u))
                    for _, u in load_queue
                )
                last_loaded = max_update_ts.isoformat(sep=" ")

                # сохраняем прогресс после каждого батча
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            if total_inserted == 0:
                self.log.info("No new timestamps.")
            else:
                self.log.info(f"Loaded {total_inserted} timestamps, last_update_ts={last_loaded}")
