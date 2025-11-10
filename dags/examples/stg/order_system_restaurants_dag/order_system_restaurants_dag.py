import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg.order_system_restaurants_dag.pg_saver_user import PgSaver as PgSaver_user
from examples.stg.order_system_restaurants_dag.pg_saver_order import PgSaver as PgSaver_order


from examples.stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader
from examples.stg.order_system_restaurants_dag.user_loader import UserLoader
from examples.stg.order_system_restaurants_dag.user_reader import UserReader
from examples.stg.order_system_restaurants_dag.order_loader import OrderLoader
from examples.stg.order_system_restaurants_dag.order_reader import OrderReader

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def sprint5_example_stg_order_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, который реализует чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver_user()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, который реализует чтение данных из источника.
        collection_reader = UserReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver_order()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, который реализует чтение данных из источника.
        collection_reader = OrderReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()


    # Последовательность выполнения — по желанию.
    # Если источники независимы, можно параллельно:
    restaurant_task = load_restaurants()
    user_task = load_users()
    order_task = load_orders()

    # type: ignore
    restaurant_task >> user_task >> order_task  # type: ignore


order_stg_dag = sprint5_example_stg_order_system()