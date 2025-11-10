ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT date_restaurant_unique UNIQUE (restaurant_id, settlement_date);