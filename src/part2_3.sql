-- DROP MATERIALIZED VIEW IF EXISTS v_periods;

-- REFRESH MATERIALIZED VIEW v_periods;

CREATE MATERIALIZED VIEW v_periods AS
  WITH Group_Purchase_Date AS
  (
    SELECT vph.customer_id AS Customer_ID ,
          vph.group_id AS Group_ID ,
          min(vph.transaction_datetime) AS First_Group_Purchase_Date ,
          max(vph.transaction_datetime) AS Last_Group_Purchase_Date ,
          count(DISTINCT vph.transaction_id) AS Group_Purchase ,
          coalesce(min(ch.sku_discount / ch.sku_summ::numeric), 0) AS Group_Min_Discount
    FROM v_purchase_history vph
    JOIN sku s ON s.group_id = vph.group_id
    LEFT JOIN checks ch ON ch.transaction_id = vph.transaction_id AND ch.sku_discount > 0 AND ch.sku_id = s.sku_id
    GROUP BY 1,2
  )
  SELECT Customer_ID ,
       Group_ID ,
       First_Group_Purchase_Date ,
       Last_Group_Purchase_Date ,
       Group_Purchase ,
       (Last_Group_Purchase_Date::date - First_Group_Purchase_Date::date + 1) / Group_Purchase::numeric AS Group_Frequency ,
       Group_Min_Discount
  FROM Group_Purchase_Date AS gpd 
  ORDER BY 1, 2;


-- Test queries
SELECT * FROM v_periods ORDER BY 1, 2;