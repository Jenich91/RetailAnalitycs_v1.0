CREATE OR REPLACE FUNCTION fnc_cross_selling_personal_offers_formation(groups_amount bigint DEFAULT 1000,
maximum_churn_rate_index numeric DEFAULT 0.5, maximum_stability_index numeric DEFAULT 0.5,
sku_maximum_share numeric DEFAULT 500,allowable_margin_share numeric DEFAULT 1)
    RETURNS TABLE ("Customer_ID" bigint, "SKU_Name" varchar, "Offer_Discount_Depth" numeric) AS $$
BEGIN
RETURN QUERY
    WITH sku_list AS
    (
        SELECT vg.customer_id
                ,vg.group_id
                ,s.sku_id
                ,dense_rank() OVER (PARTITION BY customer_id, group_id
                    ORDER BY (s.sku_retail_price - s.sku_purchase_price) DESC) AS sku_rank
                ,dense_rank() OVER (PARTITION BY customer_id
                    ORDER BY group_affinity_index DESC, group_id) AS group_rank
                ,(sku_maximum_share * (sku_retail_price - sku_purchase_price) / sku_retail_price::numeric) AS per_margin
                ,group_minimum_discount
                ,CASE
                    WHEN (group_minimum_discount * 100 % 5) = 0
                    THEN group_minimum_discount * 100
                    ELSE 5 - (group_minimum_discount * 100 % 5) + (group_minimum_discount * 100)
                END AS Offer_Discount_Depth
        FROM v_groups vg
        JOIN v_customers vc USING (customer_id)
        JOIN sku USING (group_id)
        JOIN stores s ON vc.customer_primary_store = s.transaction_store_id AND s.sku_id = sku.sku_id
        WHERE group_minimum_discount > 0
          and group_churn_rate <= maximum_churn_rate_index
          and group_stability_index < maximum_stability_index
        ORDER BY 1, group_affinity_index DESC
    )
    SELECT
        vp.customer_id
        ,sku_name
        ,Offer_Discount_Depth
    FROM v_periods vp
    JOIN sku USING (group_id)
    JOIN v_purchase_history vph USING (customer_id, group_id)
    JOIN checks ch USING (transaction_id, sku_id)
    JOIN sku_list sl ON sl.customer_id = vp.customer_id
        AND sl.group_id = vp.group_id
        AND sl.sku_id = sku.sku_id
        AND sl.sku_rank = 1
        AND sl.group_rank <= groups_amount
    WHERE
    per_margin >= Offer_Discount_Depth
    GROUP BY 1, 2, 3, vp.group_purchase
    HAVING (count(DISTINCT vph.transaction_id) / vp.group_purchase::numeric) <= allowable_margin_share
    ORDER BY 1, 2;
END;
$$ LANGUAGE PLPGSQL;

-- Test queries
select * from
fnc_cross_selling_personal_offers_formation(1, 0.5, 0.5, 500, 1);
select * from
fnc_cross_selling_personal_offers_formation(1, 0.5, 1, 500, 1);
select * from
fnc_cross_selling_personal_offers_formation(2, 0.5, 0.7, 500, 1);
select * from
fnc_cross_selling_personal_offers_formation(2, 0.5, 0.7, 50, 1);
select * from
fnc_cross_selling_personal_offers_formation(10, 1, 1, 100, 10);
