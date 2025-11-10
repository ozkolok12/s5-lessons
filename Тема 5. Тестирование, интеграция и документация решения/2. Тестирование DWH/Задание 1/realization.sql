WITH diffs AS (
SELECT *
  FROM public_test.dm_settlement_report_actual a
  FULL OUTER JOIN public_test.dm_settlement_report_expected e
    ON a.restaurant_id = e.restaurant_id
   AND a.settlement_year = e.settlement_year
   AND a.settlement_month = e.settlement_month
)
SELECT
  -- Явно делаем timestamptz с меткой +00:00
  ((NOW() AT TIME ZONE 'UTC')::text || '+00:00')::timestamptz AS test_date_time,
  'test_01'                                                   AS test_name,
  CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END            AS test_result
FROM diffs;