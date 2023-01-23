CREATE OR REPLACE VIEW v_customers AS
    WITH Customer_Average AS
    (
        SELECT DISTINCT p.customer_id AS Customer_ID ,
                     avg(t.transaction_summ) AS Customer_Average_Check ,
                     (max(t.transaction_date_time::DATE) - min(t.transaction_date_time::DATE))::NUMERIC / count(*)    AS Customer_Frequency ,
                     round(EXTRACT(EPOCH FROM ((SELECT analysis_formation FROM date_of_analysis_formation)
                                                          - max(t.transaction_date_time))) / 86400, 2)                AS Customer_Inactive_Period
        FROM personal_data p
            INNER JOIN cards c ON c.customer_id = p.customer_id
            INNER JOIN transactions t ON t.customer_card_id = c.customer_card_id
        WHERE t.transaction_date_time <= (SELECT analysis_formation
                                         FROM date_of_analysis_formation)
        GROUP BY p.customer_id
    ) 
    ,Customer_Segments AS 
    (
        SELECT ca.Customer_ID ,
          ca.Customer_Average_Check ,
          CASE
              WHEN row_number() OVER (ORDER BY Customer_Average_Check DESC) <= count(*) OVER () * 0.1 THEN 'High'
              WHEN row_number() OVER (ORDER BY Customer_Average_Check DESC) BETWEEN (count(*) OVER () * 0.1) AND (count(*) OVER () * 0.35) THEN 'Medium'
              ELSE 'Low'
          END AS Customer_Average_Check_Segment ,
          ca.Customer_Frequency ,
          CASE
              WHEN row_number() OVER (ORDER BY Customer_Frequency) <= count(*) OVER () * 0.1 THEN 'Often'
              WHEN row_number() OVER (ORDER BY Customer_Frequency) BETWEEN (count(*) OVER () * 0.1) AND (count(*) OVER () * 0.35) THEN 'Occasionally'
              ELSE 'Rarely'
          END AS Customer_Frequency_Segment ,
          ca.Customer_Inactive_Period ,
          Customer_Inactive_Period / Customer_Frequency AS Customer_Churn_Rate ,
          CASE
              WHEN (Customer_Inactive_Period / Customer_Frequency) BETWEEN 0 AND 2 THEN 'Low'
              WHEN (Customer_Inactive_Period / Customer_Frequency) BETWEEN 2 AND 5 THEN 'Medium'
              ELSE 'High'
          END AS Customer_Churn_Segment
        FROM Customer_Average ca
        ORDER BY 3
    )
    ,Customer_Store AS
    (
        SELECT
            p.customer_id,
            t.transaction_store_id,
            count(*) OVER (PARTITION BY p.customer_id ,t.transaction_store_id) / count(*) OVER (PARTITION BY p.customer_id)::NUMERIC AS Share_Of_Transactions,
            t.transaction_date_time
        FROM personal_data p
            INNER JOIN cards c ON c.customer_id = p.customer_id
            INNER JOIN transactions t ON t.customer_card_id = c.customer_card_id
        WHERE t.transaction_date_time <= (SELECT analysis_formation
                                          FROM date_of_analysis_formation)
        ORDER BY 1,4 DESC
    )
SELECT
    *
    ,(CASE
         WHEN Customer_Average_Check_Segment = 'Low' THEN 1
         WHEN Customer_Average_Check_Segment = 'Medium' THEN 10
         ELSE 19
    END)
    + (CASE
        WHEN Customer_Frequency_Segment = 'Rarely' THEN 0
        WHEN Customer_Frequency_Segment = 'Occasionally' THEN 3
        ELSE 6
    END)
    + (CASE
        WHEN Customer_Churn_Segment = 'Low' THEN 0
        WHEN Customer_Churn_Segment = 'Medium' THEN 1
        ELSE 2
    END) AS Customer_Segment ,
    CASE
        WHEN
            (
                SELECT count(DISTINCT transaction_store_id) = 1 FROM Customer_Store
                WHERE customer_id = cs.customer_id
                LIMIT 3
            ) THEN
            (
                SELECT transaction_store_id FROM Customer_Store
                WHERE customer_id = cs.customer_id
                LIMIT 1
            )
        ELSE
            (
                SELECT transaction_store_id FROM Customer_Store
                WHERE customer_id = cs.customer_id
                ORDER BY Share_Of_Transactions DESC ,transaction_date_time DESC
                LIMIT 1
            )
    END AS Customer_Primary_Store
FROM Customer_Segments cs
ORDER BY 1;

-- Test queries
SELECT * FROM v_customers;
SELECT * FROM v_customers WHERE customer_id = 1;
SELECT * FROM v_customers WHERE customer_average_check_segment = 'Low';

