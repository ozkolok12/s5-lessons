SELECT DISTINCT
    json_array_elements(event_value::JSON->'product_payments')::JSON->>'product_name'
FROM outbox;