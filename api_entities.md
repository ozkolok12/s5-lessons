### STAGING: stg.api_couriers

| Поле         | Источник        | Описание                            |
|--------------|------------------|-------------------------------------|
| id           | технический       | surrogate key                        |
| object_id    | API /couriers     | ID курьера                           |
| object_value | API /couriers     | JSON с полями courier_id, name       |

---

### STAGING: stg.api_orders

| Поле         | Источник        | Описание                            |
|--------------|------------------|-------------------------------------|
| id           | технический       | surrogate key                        |
| object_id    | API /deliveries   | ID заказа                            |
| object_value | API /deliveries   | JSON с заказом, продуктами, суммой и проч. |

---

### DDS: dds.dm_couriers

| Поле         | Источник              | Описание                           |
|--------------|------------------------|------------------------------------|
| id           | surrogate key          | Технический ID                     |
| courier_id   | stg.api_couriers       | Бизнес-ID курьера (из object_value._id) |
| courier_name | stg.api_couriers       | Имя курьера                        |

---

### DDS: dds.fct_product_sales

| Поле             | Источник            | Описание                                               |
|------------------|----------------------|--------------------------------------------------------|
| id               | surrogate key        |                                                       |
| product_id       | object_value.order_items.id | ID продукта                                  |
| order_id         | object_value._id     | ID заказа, связка с курьером и временем заказа        |
| count            | order_items.quantity | Количество единиц                                     |
| price            | order_items.price    | Цена за штуку                                         |
| total_sum        | вычисляется          | price * count                                         |
| bonus_payment    | object_value.bonus_payment | Бонус, потраченный клиентом при оплате      |
| bonus_grant      | object_value.bonus_grant | Бонус, начисленный за заказ                  |
| product_id_source| order_items.id       | Исходный ID продукта                                  |

---

### DDS: dds.dm_orders_couriers

| Поле         | Источник            | Описание                                           |
|--------------|----------------------|----------------------------------------------------|
| id           | surrogate key        |                                                    |
| order_id     | object_value._id     | ID заказа                                          |
| courier_id   | API /deliveries      | Связка курьера с заказом по полю courier_id        |
| order_ts     | object_value.date    | Дата и время создания заказа                       |
| rate         | API /deliveries      | Оценка клиента                                     |
| tip_sum      | API /deliveries      | Сумма чаевых                                       |

---

### CDM: cdm.dm_courier_ledger

| Поле                  | Источник                                               | Описание                                                                 |
|-----------------------|--------------------------------------------------------|--------------------------------------------------------------------------|
| id                    | surrogate key                                          | ID строки в витрине                                                      |
| courier_id            | dds.dm_couriers                                        | ID курьера                                                               |
| courier_name          | dds.dm_couriers                                        | Имя курьера                                                              |
| settlement_year       | dds.dm_orders_couriers.order_ts                        | Год заказа                                                               |
| settlement_month      | dds.dm_orders_couriers.order_ts                        | Месяц заказа                                                             |
| orders_count          | dds.dm_orders_couriers                                 | Кол-во уникальных заказов за период                                     |
| orders_total_sum      | dds.fct_product_sales.total_sum                        | Сумма всех заказов курьера за месяц                                     |
| rate_avg              | dds.dm_orders_couriers.rate                            | Средняя оценка курьера за месяц                                         |
| order_processing_fee  | orders_total_sum * 0.25                                | Комиссия платформы                                                      |
| courier_order_sum     | формула на основе rate_avg                             | Процент от заказа с минимальной границей                                |
| courier_tips_sum      | dds.dm_orders_couriers.tip_sum                         | Сумма чаевых                                                            |
| courier_reward_sum    | courier_order_sum + courier_tips_sum * 0.95            | Итоговая сумма к выплате курьеру (с учётом 5% комиссии на чаевые)       |
