import logging
import pendulum
from airflow.decorators import dag, task

from examples.dds.users_loader import UserLoader
from examples.dds.restaurants_loader import RestaurantLoader
from examples.dds.timestamps_loader import TimestampLoader
from examples.dds.products_loader import ProductsLoader
from examples.dds.orders_loader import OrdersLoader
from examples.dds.sales_loader import SalesLoader
from examples.dds.settlement_report_loader import SettlementReportLoader
from examples.dds.courier_loader import CouriersLoader
from examples.dds.deliveries_loader import DeliveriesLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",  # запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "dds", "origin", "users"],
    is_paused_upon_creation=True
)
def stg_dds_load():
    # Подключения
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="users_load")
    def load_users():
        users_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        users_loader.load_users()

    @task(task_id="restaurants_load")
    def load_restaurants():
        rest_loader = RestaurantLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()


    @task(task_id="timestamps_load")
    def load_timestamps():
        ts_loader = TimestampLoader(origin_pg_connect, dwh_pg_connect, log)
        ts_loader.load_timestamps()

    @task(task_id="products_load")
    def load_products():
        prod_loader = ProductsLoader(origin_pg_connect, dwh_pg_connect, log)
        prod_loader.load_products()

    @task(task_id="orders_load")
    def load_orders():
        order_loader = OrdersLoader(origin_pg_connect, dwh_pg_connect, log)
        order_loader.load_orders()

    @task(task_id="sales_load")
    def load_sales():
        sales_loader = SalesLoader(origin_pg_connect, dwh_pg_connect, log)
        sales_loader.load_sales()

    @task(task_id="settlement_report_load")
    def settlement_report_load():
        settlement_loader = SettlementReportLoader(dwh_pg_connect, log)
        settlement_loader.load()

    @task(task_id="couriers_load")
    def load_couriers():
        couriers_loader = CouriersLoader(origin_pg_connect, dwh_pg_connect, log)
        couriers_loader.load_couriers()

    @task(task_id="couriers_load")
    def load_deliveries():
        deliveries_loader = DeliveriesLoader(origin_pg_connect, dwh_pg_connect, log)
        deliveries_loader.load_deliveries()

    users_task = load_users()
    restaurants_task = load_restaurants()
    timestamps_task = load_timestamps()
    products_task = load_products()
    orders_task = load_orders() 
    sales_task = load_sales()
    settlemet_task = settlement_report_load()
    couriers_task = load_couriers()
    deliveries_task = load_deliveries()

    # Задаём порядок
    couriers_task >> deliveries_task >> users_task >> restaurants_task >> timestamps_task >> products_task >> orders_task >> sales_task >> settlemet_task 


stg_dds_load = stg_dds_load()
