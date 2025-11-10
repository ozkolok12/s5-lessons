# Основные таблицы и источники данных
## STAGING: stg.dm_couriers (новая таблица)

| Поле         | Источник        | Описание                            |
|--------------|------------------|-------------------------------------|
| id           | технический       | surrogate key                        |
| object_id    | API /couriers     | ID курьера                           |
| object_value | API /couriers     | JSON с полями courier_id, name       |

---

## STAGING: stg.fct_deliveries (новая таблица)

| Поле         | Источник        | Описание                            |
|--------------|------------------|-------------------------------------|
| id           | технический       | surrogate key                        |
| object_id    | API /deliveries   | ID заказа                            |
| object_value | API /deliveries   | JSON с заказом, продуктами, суммой и проч. |

---

## DDS: dds.dm_couriers (существующая таблица)

| Поле         | Источник              | Описание                           |
|--------------|------------------------|------------------------------------|
| id           | surrogate key          | Технический ID                     |
| courier_id   | stg.api_couriers       | Бизнес-ID курьера (из object_value._id) |
| courier_name | stg.api_couriers       | Имя курьера                        |

---

## DDS: dds.fct_product_sales (существующая таблица)

| Поле             | Источник            | Описание                                               |
|------------------|----------------------|--------------------------------------------------------|
| id               | surrogate key        |                                                       |
| product_id       | object_value.order_items.id | ID продукта                                  |
| order_id         | object_value._id     | ID заказа, внешний ключ         |
| count            | order_items.quantity | Количество единиц                                     |
| price            | order_items.price    | Цена за штуку                                         |
| total_sum        | вычисляется          | price * count                                         |
| bonus_payment    | object_value.bonus_payment | Бонус, потраченный клиентом при оплате      |
| bonus_grant      | object_value.bonus_grant | Бонус, начисленный за заказ                  |
| product_id_source| order_items.id       | Исходный ID продукта                                  |

---

## DDS: dds.fct_deliveries (новая таблица)

| Поле         | Источник            | Описание                                           |
|--------------|----------------------|----------------------------------------------------|
| id           | surrogate key        |                                                    |
| order_id     | API /deliveries     | ID заказа                                          |
| courier_id   | API /deliveries      | Связка курьера с заказом по полю courier_id        |
| order_ts     | API /deliveries    | Дата и время создания заказа                       |
| rate         | API /deliveries      | Оценка клиента                                     |
| tip_sum      | API /deliveries      | Сумма чаевых                                       |

---

## CDM: cdm.dm_courier_ledger (новая таблица)

| Поле                  | Источник                                               | Описание                                                                 |
|-----------------------|--------------------------------------------------------|--------------------------------------------------------------------------|
| id                    | surrogate key                                          | ID строки в витрине                                                      |
| courier_id            | dds.dm_couriers                                        | ID курьера                                                               |
| courier_name          | dds.dm_couriers                                        | Имя курьера                                                              |
| settlement_year       | dds.fct_deliveries.order_ts                        | Год заказа                                                               |
| settlement_month      | dds.fct_deliveries.order_ts                        | Месяц заказа                                                             |
| orders_count          | dds.fct_deliveries                                 | Кол-во уникальных заказов за период                                     |
| orders_total_sum      | dds.fct_product_sales.total_sum                        | Сумма всех заказов курьера за месяц                                     |
| rate_avg              | dds.fct_deliveries.rate                            | Средняя оценка курьера за месяц                                         |
| order_processing_fee  | orders_total_sum * 0.25                                | Комиссия платформы                                                      |
| courier_order_sum     | формула на основе rate_avg                             | Процент от заказа с минимальной границей                                |
| courier_tips_sum      | dds.fct_deliveries.tip_sum                         | Сумма чаевых                                                            |
| courier_reward_sum    | courier_order_sum + courier_tips_sum * 0.95            | Итоговая сумма к выплате курьеру (с учётом 5% комиссии на чаевые)       |

# DBML схема

```dbml
    // ===================
    // STAGING 
    // ===================

    Table stg.ordersystem_deliveries {
    id int [pk]
    object_id varchar
    object_value varchar [note: 'Данные из API']
    }

    Table stg.ordersystem_couriers {
    id int [pk]
    object_id varchar
    object_value varchar [note: 'Данные из API']
    }

    // ==============
    // DDS 
    // ==============

    Table dds.dm_couriers {
    id serial [pk, note: 'Surrogate key']
    courier_id varchar [unique, note: 'Business key — ID из API']
    courier_name varchar [note: 'ФИО курьера']
    }

    Table dds.fct_product_sales {
    id serial [pk]
    product_id int
    order_id varchar [unique]
    count int
    price NUMERIC (14,2)
    total_sum NUMERIC (14,2)
    bonus_payment NUMERIC (14,2)
    bonus_grant NUMERIC (14,2)
    product_id_source varchar
    }

    Table dds.fct_deliveries {
    id serial [pk]
    order_id varchar [note: 'ID заказа для связи с fct_product_sales']
    courier_id varchar [note: 'ID курьера для связи с dm_couriers']
    order_ts timestamp [note: 'Дата создания заказа (используется в витрине для определения месяца)']
    rate int [note: 'Оценка клиента (используется для расчёта средней оценки за месяц)']
    tip_sum numeric(14,2) [note: 'Сумма чаевых за заказ']
    }

    // ==============
    // CDM 
    // ==============

    Table cdm.dm_courier_ledger {
    id serial [pk, note: 'Surrogate key']

    courier_id varchar [note: 'Из dds.dm_couriers']
    courier_name varchar [note: 'Из dds.dm_couriers']

    settlement_year int [note: 'Год из поля order_ts']
    settlement_month int [note: 'Месяц из поля order_ts ']

    orders_count int [note: 'Количество уникальных order_id за месяц из dds.fct_deliveries']
    orders_total_sum numeric(14,2) [note: 'Сумма заказов из dds.fct_product_sales по order_id']
    rate_avg float [note: 'Среднее значение поля rate из dds.fct_deliveries по courier_id и месяцу']

    order_processing_fee numeric(14,2) [note: 'orders_total_sum * 0.25']

    courier_order_sum numeric(14,2) [note: 'Вычисляется по правилам в зависимости от rate_avg:\n< 4 → 5% (min 100₽), 4–4.5 → 7% (min 150₽), 4.5–4.9 → 8% (min 175₽), ≥4.9 → 10% (min 200₽)']

    courier_tips_sum numeric(14,2) [note: 'Сумма поля tip_sum из dds.dm_orders']

    courier_reward_sum numeric(14,2) [note: 'courier_order_sum + courier_tips_sum * 0.95\n(5% комиссия за обработку платежа)']
    }

    // ==========================
    // RELATIONSHIPS
    // ==========================

    Ref: dds.dm_couriers.courier_id > dds.fct_deliveries.courier_id

    Ref: dds.fct_product_sales.order_id > dds.fct_deliveries.order_id

    Ref: cdm.dm_courier_ledger.courier_id > dds.dm_couriers.courier_id
````


