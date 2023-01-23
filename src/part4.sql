CREATE OR REPLACE FUNCTION fnc_growth_the_average_check(pay_method_ integer default 1, 
                                                        first_date_ date default '2022-08-01',
                                                        last_date_ date default '2022-08-17',
                                                        transaction_number_ integer default 5,
                                                        coef_increase_avg_check_ numeric default 1.5,
                                                        max_churn_index_ numeric default 10,
                                                        max_share_transaction_discount_ numeric default 70,
                                                        allow_share_margin_ numeric default 50)
RETURNS TABLE(  "Customer_ID" bigint,
                "Required_Check_Measure" numeric,
                "Group_Name" varchar,
                "Offer_Discount_Depth" numeric)
AS $$
BEGIN
    IF(pay_method_ != 1 AND pay_method_ != 2) THEN RAISE EXCEPTION 'Error: Pls, use 1 - for period, 2 - for number of transactions';
    END IF;

    IF ((first_date_) < ((SELECT transaction_date_time FROM transactions ORDER BY transaction_date_time LIMIT 1)::date)
        OR (first_date_ > (SELECT analysis_formation FROM date_of_analysis_formation)::date)) THEN first_date_ := (SELECT transaction_date_time FROM transactions ORDER BY transaction_date_time LIMIT 1)::date;
    END IF;
    IF ((last_date_) < ((SELECT transaction_date_time FROM transactions ORDER BY transaction_date_time LIMIT 1)::date)
        OR (last_date_ > (SELECT analysis_formation FROM date_of_analysis_formation)::date)) THEN last_date_ := (SELECT analysis_formation FROM date_of_analysis_formation)::date;
    END IF;
    IF (first_date_ > last_date_) THEN RAISE EXCEPTION 'Error: First date is older than Last date';
    END IF;

    IF (transaction_number_ <= 0) THEN RAISE EXCEPTION 'Error: The number of recent transactions should be > 0';
    END IF;
    IF (coef_increase_avg_check_ <= 0) THEN RAISE EXCEPTION 'Error: The coefficient of average check increase should be > 0';
    END IF;
    IF (max_churn_index_ <= 0) THEN RAISE EXCEPTION 'Error: The maximum churn index should be > 0';
    END IF;
    IF (max_share_transaction_discount_ <= 0) THEN RAISE EXCEPTION 'Error: The maximum share of transactions with a discount (in percent) should be > 0';
    END IF;
    IF (allow_share_margin_ <= 0) THEN RAISE EXCEPTION 'Error: The allowable share of margin (in percent) should be > 0';
    END IF;

    RETURN QUERY   
    WITH rcm_by_period AS (
        SELECT c.customer_id,(avg(t.transaction_summ)*coef_increase_avg_check_) AS p_required_check_measure
        FROM cards c
            INNER JOIN transactions t ON  c.customer_card_id = t.customer_card_id
        WHERE t.transaction_date_time BETWEEN first_date_ AND last_date_
        GROUP BY 1
        ORDER BY 1
    ),
    rcm_by_number_transactions AS (
        WITH sub_1 AS (
             SELECT c.customer_id AS customer_id,
                    t.transaction_summ AS transaction_summ,
                    t.transaction_date_time AS transaction_date_time,
                    row_number() OVER(PARTITION BY c.customer_id ORDER BY c.customer_id, t.transaction_date_time DESC) AS number_transaction
             FROM cards c
                 INNER JOIN transactions t ON  c.customer_card_id = t.customer_card_id
        )
        SELECT  sub_1.customer_id AS customer_id,
                (avg(sub_1.transaction_summ)*coef_increase_avg_check_) AS n_required_check_measure
        FROM sub_1
        WHERE sub_1.number_transaction <= transaction_number_
        GROUP BY 1
    ),
    list_group AS (
        SELECT  customer_id,
                group_id,
                dense_rank() OVER (PARTITION BY customer_id ORDER BY group_affinity_index DESC) AS rnk
        FROM v_groups
        WHERE Group_Churn_Rate <= max_churn_index_ AND Group_Discount_Share < (max_share_transaction_discount_/100::numeric)
    ),
    list_group_with_max_discount AS(
        SELECT  DISTINCT
                lg.customer_id,
                lg.group_id,
                lg.rnk,
                ((avg(vph.group_summ_paid - vph.group_cost) OVER (PARTITION BY vph.customer_id, vph.group_id)) * (allow_share_margin_/100::numeric)) as max_limit_discount,
                CASE
                    WHEN ceil(vg.Group_Minimum_Discount * 100) % 5 = 0
                        THEN ceil(vg.Group_Minimum_Discount * 100)
                    ELSE (ceil(vg.Group_Minimum_Discount * 100 + 5) - ceil(vg.Group_Minimum_Discount * 100 + 5) % 5)
                END AS amount_discount
        FROM list_group lg
            INNER JOIN v_groups vg ON lg.customer_id = vg.customer_id AND lg.group_id = vg.group_id
            INNER JOIN v_purchase_history vph ON lg.customer_id = vph.customer_id AND lg.group_id = vph.group_id
        WHERE vg.Group_Minimum_Discount > 0
    ),
    final_table AS (
    SELECT  pd.customer_id                                                                          AS Customer_ID,
        CASE
            WHEN pay_method_ = 1 THEN rcm_by_period.p_required_check_measure
            WHEN pay_method_ = 2 THEN rcm_by_number_transactions.n_required_check_measure
        END                                                                                         AS Required_Check_Measure,
        group_name                                                                                  AS Group_Name,
        lgwmd_1.amount_discount                                                                     AS Offer_Discount_Depth
    FROM personal_data pd
        FULL JOIN rcm_by_period ON pd.customer_id = rcm_by_period.customer_id
        FULL JOIN rcm_by_number_transactions ON pd.customer_id = rcm_by_number_transactions.customer_id
        FULL JOIN  list_group_with_max_discount lgwmd_1 ON pd.customer_id = lgwmd_1.customer_id
        FULL JOIN sku_groups sg ON sg.group_id = lgwmd_1.group_id
    WHERE lgwmd_1.rnk = (SELECT min(lgwmd_2.rnk)
                        FROM list_group_with_max_discount lgwmd_2
                        WHERE lgwmd_2.customer_id = lgwmd_1.customer_id AND lgwmd_2.amount_discount < lgwmd_2.max_limit_discount)
    )
    SELECT DISTINCT *
    FROM final_table
    WHERE Required_Check_Measure IS NOT NULL;

END;
$$ LANGUAGE plpgsql;


-- Test queries
SELECT * FROM fnc_growth_the_average_check();

SELECT * FROM fnc_growth_the_average_check(1,'2015-08-01','2023-08-22',1,1.5,1.3, 100., 70.);

SELECT * FROM fnc_growth_the_average_check(1,'2018-01-20','2022-08-22',1,1.5,50,75,35);
SELECT * FROM fnc_growth_the_average_check(1,'2022-08-01','2022-08-22',1,1.5,10,75,35);
SELECT * FROM fnc_growth_the_average_check(1,'2021-10-01','2021-11-23',5,1.5,10,75,35);

SELECT * FROM fnc_growth_the_average_check(2,'2021-10-01','2021-11-23',5,1.5,10,75,35);
SELECT * FROM fnc_growth_the_average_check(2,'2021-10-01','2021-11-23',10,1.5,10,75,35);
SELECT * FROM fnc_growth_the_average_check(2,'2021-10-01','2021-11-23',100,1.5,50,25,15);
