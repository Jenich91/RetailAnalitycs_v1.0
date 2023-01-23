---------- DROP INDEX ----------
DROP INDEX IF EXISTS idx_v_periods;
DROP INDEX IF EXISTS idx_v_purchase_history_Customer_ID;
DROP INDEX IF EXISTS idx_v_purchase_history_Group_ID;
DROP INDEX IF EXISTS idx_v_purchase_history_Transaction_DateTime;

---------- CREATE INDEX ----------
CREATE INDEX IF NOT EXISTS idx_v_periods ON v_periods(Customer_ID, Group_ID);
CREATE INDEX IF NOT EXISTS idx_v_purchase_history_Customer_ID ON v_purchase_history(Customer_ID);
CREATE INDEX IF NOT EXISTS idx_v_purchase_history_Group_ID ON v_purchase_history(Group_ID);
CREATE INDEX IF NOT EXISTS idx_v_purchase_history_Transaction_DateTime ON v_purchase_history(Transaction_DateTime);

CREATE DOMAIN MODE AS integer CHECK (VALUE BETWEEN 1 AND 2);
CREATE DOMAIN uint AS integer CHECK (VALUE BETWEEN 0 AND 32767);

CREATE OR REPLACE FUNCTION fnc_create_groups_view(selector_ MODE DEFAULT 1, amount_ uint DEFAULT 1000)
RETURNS TABLE (
    "Customer_ID" bigint
    , "Group_ID" bigint
    , "Group_Affinity_Index" numeric
    , "Group_Churn_Rate" numeric
    , "Group_Stability_Index" numeric
    , "Group_Margin" numeric
    , "Group_Discount_Share" numeric
    , "Group_Min_Discount" numeric
    , "Group_Average_Discount" numeric)
AS $$
DECLARE
    date_analysis_formation date := (SELECT analysis_formation FROM date_of_analysis_formation);
BEGIN
    IF selector_ IN (1, 2) AND amount_ > 0 THEN
    RETURN QUERY
        WITH transactions_at_discount AS
        (
            SELECT
                vph.customer_id
                ,vph.group_id
                ,count(DISTINCT vph.transaction_id) AS count_transactions

            FROM v_purchase_history vph
            JOIN sku s ON s.group_id = vph.group_id
            JOIN checks ch ON ch.transaction_id = vph.transaction_id AND ch.sku_id = s.sku_id AND ch.sku_discount > 0
            GROUP BY vph.customer_id, vph.group_id
            ORDER BY 1, 2
        )
        ,Cte_Affinity_Index AS
        (
            SELECT
                vp.customer_id
                ,vp.group_id
                ,(vp.group_purchase / count(DISTINCT vph.transaction_id)::numeric) AS Group_Affinity_Index
            FROM v_periods vp
            JOIN v_purchase_history vph USING (customer_id)
            WHERE vph.transaction_datetime BETWEEN vp.First_Group_Purchase_Date AND vp.last_group_purchase_date
            GROUP BY vp.customer_id, vp.group_id, vp.group_purchase
        )
        ,Relative_Deviation AS
        (
            SELECT
                vp.customer_id
                ,vp.group_id
                ,vph.transaction_datetime
                ,vph.group_summ_paid
                ,group_cost
                ,row_number() OVER (PARTITION BY vph.Customer_ID, vph.Group_ID ORDER BY transaction_datetime DESC)  AS row_count
                ,((date_analysis_formation - vp.Last_Group_Purchase_Date::date) / vp.group_frequency::numeric)                          AS Group_Churn_Rate
                ,abs(transaction_datetime::date
                    - LAG(transaction_datetime) OVER (PARTITION BY vph.Customer_ID, vph.Group_ID ORDER BY transaction_datetime)::date
                    - vp.group_frequency)
                / vp.group_frequency                                                                                AS Deviation
                ,avg(vph.Group_Summ_Paid/vph.Group_Summ::numeric) over(partition by customer_id, group_id)                                                      AS Group_Average_Discount
                ,vp.group_min_discount                                                                              AS Group_Minimum_Discount
                ,coalesce((td.count_transactions / vp.group_purchase::numeric), 0)                          AS Group_Discount_Share

            FROM v_purchase_history vph
            JOIN v_periods vp USING (Customer_ID, Group_ID)
            LEFT JOIN transactions_at_discount td USING (Customer_ID, Group_ID)
        )
        SELECT DISTINCT
            customer_id
            ,group_id
            ,cai.group_affinity_index
            ,rd.group_churn_rate
            ,(coalesce(avg(rd.Deviation) OVER w_part_customerID_groupID, 0))                                AS Group_Stability_Index
            ,sum(CASE
                    WHEN selector_ = 1 AND
                            transaction_datetime BETWEEN date_analysis_formation - amount_ AND date_analysis_formation
                        THEN Group_Summ_Paid - Group_Cost
                    WHEN selector_ = 2 AND row_count <= amount_
                        THEN Group_Summ_Paid - Group_Cost
                    ELSE 0
                END ) OVER w_part_customerID_groupID                                                                AS Group_Margin
            ,group_discount_share
            ,group_minimum_discount
            ,group_average_discount

        FROM Relative_Deviation rd
        JOIN Cte_Affinity_Index cai USING (customer_id, group_id)
        WINDOW w_part_customerID_groupID AS (PARTITION BY rd.Customer_ID, rd.Group_ID);
    END IF;
END;
$$ LANGUAGE PLPGSQL;

---------- DROP MATERIALIZED VIEW ----------
-- DROP MATERIALIZED VIEW IF EXISTS v_groups;

CREATE MATERIALIZED VIEW v_groups
    (
        Customer_ID,
        Group_ID,
        Group_Affinity_Index,
        Group_Churn_Rate,
        Group_Stability_Index,
        Group_Margin,
        Group_Discount_Share,
        Group_Minimum_Discount,
        Group_Average_Discount
    ) AS
    SELECT *
    FROM fnc_create_groups_view();



-- Test queries
SELECT * FROM v_groups;

SELECT * FROM fnc_create_groups_view(1, 100);
SELECT * FROM fnc_create_groups_view(2, 100);
SELECT * FROM fnc_create_groups_view(1, 1000);
SELECT * FROM fnc_create_groups_view(2, 1000);
SELECT * FROM fnc_create_groups_view(1, 7);