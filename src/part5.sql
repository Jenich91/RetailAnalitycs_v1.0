CREATE OR REPLACE FUNCTION part5 (first_date timestamp, last_date timestamp, added_transactions int, max_churn_rate numeric, max_discount_share numeric, max_margin numeric) RETURNS TABLE ("Customer_ID" bigint ,"Start_Date" timestamp ,"End_Date" timestamp ,"Required_Transactions_Count" int ,"Group_Name" varchar ,"Offer_Discount_Depth" numeric) AS $$
    WITH AllGroup AS
    (
        SELECT DISTINCT
            vc.customer_id
            ,vg.group_id
            ,vg.group_affinity_index
            ,round((last_date::date - first_date::date) / vc.customer_frequency) + added_transactions       AS Required_Transactions_Count
            ,avg(vph.group_summ_paid - vph.group_cost) OVER (PARTITION BY vph.customer_id, vph.group_id)
                        / 100 * max_margin                                                                  AS Margin
            ,CASE
                WHEN (vg.group_minimum_discount * 100 % 5) = 0
                THEN vg.group_minimum_discount * 100
                ELSE 5 - (vg.group_minimum_discount * 100 % 5) + (vg.group_minimum_discount * 100)
            END                                                                                             AS Offer_Discount_Depth

        FROM v_customers vc
        JOIN v_groups vg USING (customer_id)
        JOIN v_purchase_history vph USING (customer_id, group_id)
        WHERE group_churn_rate <= max_churn_rate
            AND group_discount_share * 100 < max_discount_share
            AND group_minimum_discount > 0
    )

    SELECT
        customer_id
        ,first_date
        ,last_date
        ,required_transactions_count
        ,group_name
        ,offer_discount_depth

    FROM AllGroup ag
    JOIN sku_groups USING (group_id)
    WHERE ag.Group_Affinity_Index =
        (
            SELECT DISTINCT
                max(Group_Affinity_Index) FROM AllGroup
            WHERE customer_id = ag.customer_id
            AND Offer_Discount_Depth < Margin
        )
    ORDER BY 1
$$ LANGUAGE SQL;

-- Test queries
SELECT * FROM part5('2021-01-10 10:07:00', '2022-06-28 12:12:12', 1, 0.9, 55., 50.);
SELECT * FROM part5('2018-01-16 10:07:00', '2020-08-28 12:12:12', 10, 0.5, 100., 70.);
SELECT * FROM part5('2018-01-16 10:07:00', '2019-08-28 12:12:12', 111, 0.5, 50., 45.);
SELECT * FROM part5('2018-01-16 10:07:00', '2021-08-28 12:12:12', 63, 0.7, 70., 25.);
SELECT * FROM part5('2019-01-16 10:07:00', '2019-08-28 12:12:12', 14, 1., 40., 55.);