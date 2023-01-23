-- DROP MATERIALIZED VIEW IF EXISTS v_purchase_history CASCADE;

-- REFRESH MATERIALIZED VIEW v_purchase_history;

CREATE MATERIALIZED VIEW v_purchase_history AS
SELECT pd.customer_id ,
       t.transaction_id ,
       t.transaction_date_time AS transaction_datetime ,
       sku.group_id ,
       sum(s.sku_purchase_price * ch.sku_amount) AS group_cost ,
       sum(ch.sku_summ) AS group_summ ,
       sum(ch.sku_summ_paid) AS group_summ_paid
FROM personal_data pd
  JOIN cards c ON pd.customer_id = c.customer_id
  JOIN transactions t ON t.customer_card_id = c.customer_card_id
  JOIN checks ch ON ch.transaction_id = t.transaction_id
  JOIN sku ON sku.sku_id = ch.sku_id
  JOIN stores s ON t.transaction_store_id = s.transaction_store_id AND s.sku_id = sku.sku_id
GROUP BY 1, 2, 3, 4;

-- Test queries
SELECT *
FROM v_purchase_history
ORDER BY transaction_datetime;

SELECT *
FROM v_purchase_history
WHERE transaction_datetime < '2020-01-01 00:00:00';

SELECT *
FROM v_purchase_history
WHERE transaction_datetime BETWEEN '2019-01-01 00:00:00' AND '2020-01-01 00:00:00';

SELECT *
FROM v_purchase_history
WHERE (transaction_datetime BETWEEN '2019-01-01 00:00:00' AND '2020-01-01 00:00:00')
  AND customer_id = 1;