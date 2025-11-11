import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
import json


from examples.stg.couriers_deliveries_stg_dag.pg_saver import PgSaver
from examples.stg.couriers_deliveries_stg_dag.courier_reader import CourierReader
from examples.stg.couriers_deliveries_stg_dag.courier_reader import CourierReader as DeliveriesReader

from examples.stg import StgEtlSettingsRepository
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=True,
    tags=['sprint5', 'example', 'stg', 'origin'],
    is_paused_upon_creation=False
) 
def sprint5_stg_couriers_deliveries():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    pg_conn = dwh_pg_connect.client()

    COURIER_API_COHORT = Variable.get("COURIER_API_COHORT")
    COURIER_API_KEY = Variable.get("COURIER_API_KEY")
    COURIER_API_NICK = Variable.get("COURIER_API_NICK")
    
    WF_KEY_COURIERS = "stg_couriers_offset"
    WF_KEY_DELIVERIES = "stg_deliveries_offset"
    DAYS_BACK = 7

    LIMIT = 50

    @task()
    def load_couriers():
        # Инициализируем нужные классы
        # Чтение по АПИ
        reader = CourierReader(
            COURIER_API_NICK,
            COURIER_API_COHORT,
            COURIER_API_KEY
            )
        # Сохранение в БД
        saver = PgSaver(dwh_pg_connect)

        # Чекпоинт таблица
        settings_repo = StgEtlSettingsRepository()

        setting = settings_repo.get_setting(pg_conn, WF_KEY_COURIERS)
        if setting is None:
            offset = 0
            settings_repo.save_setting(
                pg_conn,
                WF_KEY_COURIERS,
                json.dumps({"offset": offset})
                )
        else:
            offset = setting.workflow_settings.get("offset", 0)
        
        # Загружаем данные по АПИ
        rows = reader.load_couriers(offset=offset)
        if not rows:
            log.info("[COURIERS] No new rows to load.")
            return

        # Сохраняем в БД
        saver.save_couriers(rows)
        new_offset = offset + LIMIT
        settings_repo.save_setting(
            pg_conn,
            WF_KEY_COURIERS,
            json.dumps({"offset": new_offset})
            )

        log.info(f"[COURIERS] Загрузили  {len(rows)} строк, новый offset: {new_offset}")

    @task()
    def load_deliveries():
        reader = DeliveriesReader(COURIER_API_NICK, COURIER_API_COHORT, COURIER_API_KEY)
        saver = PgSaver(dwh_pg_connect)
        settings_repo = StgEtlSettingsRepository()

        setting = settings_repo.get_setting(pg_conn, WF_KEY_DELIVERIES)
        offset = 0 if setting is None else setting.workflow_settings.get("offset", 0)
        if setting is None:
            settings_repo.save_setting(pg_conn, WF_KEY_DELIVERIES, json.dumps({"offset": offset}))

        from_date = pendulum.now("UTC").subtract(days=DAYS_BACK).format("YYYY-MM-DD 00:00:00")

        rows = reader.load_deliveries(offset=offset, from_date=from_date)
        if not rows:
            log.info("[DELIVERIES] Нечего грузить.")
            return

        saver.save_deliveries(rows)
        new_offset = offset + LIMIT
        settings_repo.save_setting(pg_conn, WF_KEY_DELIVERIES, json.dumps({"offset": new_offset}))
        log.info(f"[DELIVERIES] {len(rows)} строк , новый offset: {new_offset}, from={from_date}")

    couriers_task = load_couriers()
    deliveries_task = load_deliveries()

    couriers_task >> deliveries_task


couriers_deliveries_stg_dag = sprint5_stg_couriers_deliveries()