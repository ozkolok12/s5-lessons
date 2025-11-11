import logging
from logging import getLogger

import pendulum
from airflow.decorators import dag, task

from lib import ConnectionBuilder
from examples.cdm.dm_courier_ledger_loader import CourierMonthlyMartLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval="0/15 * * * *",                
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,                          
    max_active_runs=1,
    tags=["sprint5", "cdm"],
    is_paused_upon_creation=False,
)

def sprint5_cdm_insert():
    @task()
    def cdm_loader():
        logger = getLogger("cdm_courier_monthly")
        dwh_pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        loader = CourierMonthlyMartLoader(dwh_pg, logger)
        loader.load()

    cdm_loader()

dag = sprint5_cdm_insert()