delete from tutorial.LCS_member_KPI_YTD;  -- for the subsequent update
insert into tutorial.LCS_member_KPI_YTD

WITH all_purchase AS (
SELECT DISTINCT trans.crm_member_id,
       trans.eff_reg_channel,
       trans.original_order_id,
       orders.order_paid_time
FROM edw.f_member_order_detail trans
LEFT JOIN (SELECT original_store_code, 
                  original_order_id,
                  MIN(order_paid_time) AS order_paid_time
            FROM edw.f_lcs_order_detail
             GROUP BY  1,2
        ) orders
      ON trans.original_order_id = CONCAT(orders.original_store_code, orders.original_order_id)
    WHERE is_rrp_sales_type = 1 
      AND if_eff_order_tag IS TRUE
      AND distributor_name LIKE '%LCS%'  -- distributor_name
),

all_purchase_rk AS(
SELECT crm_member_id,
        eff_reg_channel,
        original_order_id,
        order_paid_time,
        ROW_NUMBER () OVER (PARTITION BY crm_member_id ORDER BY order_paid_time ASC) AS rk
FROM all_purchase
),


new_member_base AS (
SELECT DISTINCT member_detail_id, eff_reg_channel
            FROM edw.d_member_detail 
           WHERE 1=1
             AND DATE(join_time) >= '2024-01-01'            -- start_date
             AND DATE(join_time) < current_date             -- end_date
             AND UPPER(eff_reg_channel) LIKE '%LCS%'   -- distributor_name
),

existing_never_purchased_base AS (
  SELECT DISTINCT mbr.member_detail_id
    FROM edw.d_member_detail mbr
    LEFT JOIN (           SELECT DISTINCT crm_member_id::integer
                     FROM edw.f_member_order_detail 
                    WHERE is_rrp_sales_type = 1 
                      AND if_eff_order_tag IS TRUE
                      AND distributor_name <> 'LBR'
                      AND date_id < '2024-01-01'   -- start_date
                ) purchased
           ON mbr.member_detail_id::integer = purchased.crm_member_id::integer
   WHERE DATE(join_time) < '2024-01-01'            -- start_date
     AND UPPER(eff_reg_channel) LIKE '%LCS%'  -- distributor_name
     AND purchased.crm_member_id IS NULL
),


existing_retain_base AS (
  SELECT DISTINCT crm_member_id, distributor_name
             FROM edw.f_member_order_detail 
            WHERE is_rrp_sales_type = 1 
              AND if_eff_order_tag IS TRUE
              AND distributor_name <> 'LBR'
              AND distributor_name LIKE '%LCS%'   -- distributor_name
              AND date_id >= '2023-01-01'  -- fixed
              AND date_id < '2024-01-01'   -- fixed
),


existing_reactivate_base AS (
  SELECT DISTINCT trans.crm_member_id, distributor_name
             FROM edw.f_member_order_detail trans
        LEFT JOIN (SELECT DISTINCT crm_member_id::integer FROM existing_retain_base) retain
               ON trans.crm_member_id::integer = retain.crm_member_id::integer
            WHERE is_rrp_sales_type = 1 
              AND if_eff_order_tag IS TRUE
              AND distributor_name <> 'LBR'
              AND distributor_name LIKE '%LCS%'    -- distributor_name
              AND date_id < '2023-01-01'        -- fixed
              AND retain.crm_member_id IS NULL  
),


existing_2024_initial_purchase_base AS (
  SELECT DISTINCT crm_member_id
    FROM all_purchase_rk   -- 已过滤distributor
 WHERE DATE(order_paid_time) >= '2024-01-01' -- fixed
   AND DATE(order_paid_time) < '2024-01-01'          -- start_date
   AND rk = 1
),


existing_other_channel_reg_new_belong AS (
  SELECT DISTINCT crm_member_id
    FROM all_purchase_rk
LEFT JOIN new_member_base
      ON all_purchase_rk.crm_member_id::integer = new_member_base.member_detail_id::integer
 WHERE DATE(order_paid_time) >= '2024-01-01'            -- start_date
   AND DATE(order_paid_time) < current_date            -- end_date
   AND UPPER(all_purchase_rk.eff_reg_channel) NOT LIKE '%LCS%'  -- distributor_name 
   AND rk = 1
   AND new_member_base.member_detail_id IS NULL
),

existing_3_month_purchased_base AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= '2024-01-01' - 90 AND order_paid_time < '2024-01-01'  THEN 1 ELSE 0 END) AS if_3_month_not_purchased -- [-90, 0)
 FROM all_purchase -- 已过滤distributor
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= '2024-01-01' - 90 AND order_paid_time < '2024-01-01' THEN 1 ELSE 0 END) = 1   -- start_date
),

existing_3_month_not_purchased_base AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= '2024-01-01' - 90 AND order_paid_time < '2024-01-01' THEN 1 ELSE 0 END) AS if_3_month_not_purchased, -- [-90, 0)
       MAX(CASE WHEN order_paid_time >= '2024-01-01' - 180 AND order_paid_time < '2024-01-01' - 90 THEN 1 ELSE 0 END) AS if_3_6_month_purchased  -- [-180, -90)
 FROM all_purchase -- 已过滤distributor
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= '2024-01-01' - 90 AND order_paid_time < '2024-01-01'  THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time >= '2024-01-01' - 180 AND order_paid_time < '2024-01-01' - 90 THEN 1 ELSE 0 END) = 1
),



existing_6_month_not_purchased_base AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= '2024-01-01'  - 180 AND order_paid_time < '2024-01-01' THEN 1 ELSE 0 END) AS if_6_month_not_purchased, -- [-180, -90)
       MAX(CASE WHEN order_paid_time >= '2024-01-01'  - 270 AND order_paid_time < '2024-01-01' - 180 THEN 1 ELSE 0 END) AS if_6_9_month_purchased  -- [-270, -180)
 FROM all_purchase -- 已过滤distributor
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= '2024-01-01' - 180 AND order_paid_time < '2024-01-01'  THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time >= '2024-01-01'  - 270 AND order_paid_time < '2024-01-01' - 180 THEN 1 ELSE 0 END) = 1
),


existing_9_month_not_purchased_base AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= '2024-01-01' - 270 AND order_paid_time < '2024-01-01' THEN 1 ELSE 0 END) AS if_9_month_not_purchased, -- [-270, -0)
       MAX(CASE WHEN order_paid_time >= '2024-01-01' - 360 AND order_paid_time < '2024-01-01' - 270 THEN 1 ELSE 0 END) AS if_9_12_month_purchased  -- [-360, -270)
 FROM all_purchase -- 已过滤distributor
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= '2024-01-01'- 270 AND order_paid_time < '2024-01-01' THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time >= '2024-01-01' - 360 AND order_paid_time < '2024-01-01' - 270 THEN 1 ELSE 0 END) = 1
),


existing_12_month_and_longer_not_purchased_base AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= '2024-01-01' - 360 AND order_paid_time < '2024-01-01' THEN 1 ELSE 0 END)      AS if_12_month_not_purchased, -- [-360, -0)
       MAX(CASE WHEN order_paid_time < '2024-01-01' - 360 THEN 1 ELSE 0 END)                                             AS if_before_12_month_purchased  -- [-360, -270)
 FROM all_purchase -- 已过滤distributor
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= '2024-01-01' - 360 AND order_paid_time < '2024-01-01' THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time < '2024-01-01' - 360 THEN 1 ELSE 0 END)  = 1
),


have_not_retained AS (
  SELECT DISTINCT retain_base.crm_member_id
             FROM edw.f_member_order_detail retain_base
        LEFT JOIN (SELECT DISTINCT crm_member_id FROM all_purchase WHERE order_paid_time >= '2024-01-01' AND order_paid_time < current_date) retained -- end_date
               ON retain_base.crm_member_id = retained.crm_member_id
            WHERE is_rrp_sales_type = 1 
              AND if_eff_order_tag IS TRUE
              AND distributor_name <> 'LBR'
              AND distributor_name LIKE '%LCS%' -- distributor_name
              AND date_id >= '2023-01-01'  -- fixed
              AND date_id < '2024-01-01'   -- fixed
              AND retained.crm_member_id IS NULL
),



have_not_reactivated AS (
  SELECT DISTINCT trans.crm_member_id, distributor_name
             FROM edw.f_member_order_detail trans
        LEFT JOIN (SELECT DISTINCT crm_member_id::integer FROM existing_retain_base) retain
               ON trans.crm_member_id::integer = retain.crm_member_id::integer
          LEFT JOIN (SELECT DISTINCT crm_member_id FROM all_purchase WHERE order_paid_time >= '2024-01-01' AND order_paid_time < current_date) reactivated -- end_date
               ON trans.crm_member_id = reactivated.crm_member_id
            WHERE is_rrp_sales_type = 1 
              AND if_eff_order_tag IS TRUE
              AND distributor_name <> 'LBR'
              AND distributor_name LIKE '%LCS%'  -- distributor_name
              AND date_id < '2023-01-01'      -- fixed
              AND retain.crm_member_id IS NULL 
              AND reactivated.crm_member_id IS NULL
),

have_not_purchased_3_month AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= current_date - 90 AND order_paid_time < current_date THEN 1 ELSE 0 END) AS if_3_month_not_purchased, -- [-90, 0)
       MAX(CASE WHEN order_paid_time >= current_date - 180 AND order_paid_time < current_date - 90 THEN 1 ELSE 0 END) AS if_3_6_month_purchased  -- [-180, -90)
 FROM all_purchase
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= current_date - 90 AND order_paid_time < current_date THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time >= current_date - 180 AND order_paid_time < current_date - 90 THEN 1 ELSE 0 END) = 1        -- end_date
),



have_not_purchased_6_month AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= current_date - 180 AND order_paid_time < current_date THEN 1 ELSE 0 END) AS if_6_month_not_purchased, -- [-180, -90)
       MAX(CASE WHEN order_paid_time >= current_date - 270 AND order_paid_time < current_date - 180 THEN 1 ELSE 0 END) AS if_6_9_month_purchased  -- [-270, -180)
 FROM all_purchase
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= current_date - 180 AND order_paid_time < current_date THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time >= current_date - 270 AND order_paid_time < current_date - 180 THEN 1 ELSE 0 END) = 1
),


have_not_purchased_9_month AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= current_date - 270 AND order_paid_time < current_date THEN 1 ELSE 0 END) AS if_9_month_not_purchased, -- [-270, -0)
       MAX(CASE WHEN order_paid_time >= current_date - 360 AND order_paid_time < current_date - 270 THEN 1 ELSE 0 END) AS if_9_12_month_purchased  -- [-360, -270)
 FROM all_purchase
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= current_date - 270 AND order_paid_time < current_date  THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time >= current_date - 360 AND order_paid_time < current_date - 270 THEN 1 ELSE 0 END) = 1
),


have_not_purchased_12_month_and_longer AS (
SELECT crm_member_id,
       MAX(CASE WHEN order_paid_time >= current_date - 360 AND order_paid_time < current_date THEN 1 ELSE 0 END)      AS if_12_month_not_purchased, -- [-360, -0)
       MAX(CASE WHEN order_paid_time < current_date - 360 THEN 1 ELSE 0 END)                                             AS if_before_12_month_purchased  -- [-360, -270)
 FROM all_purchase
 GROUP BY 1
 HAVING MAX(CASE WHEN order_paid_time >= current_date - 360 AND order_paid_time < current_date THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_paid_time < current_date - 360 THEN 1 ELSE 0 END)  = 1
),


YTD_member_shopper_and_repeat_shopper AS (
SELECT DISTINCT trans.crm_member_id,
       CASE WHEN YTD_repeat.crm_member_id IS NULL THEN 0 ELSE 1 END AS YTD_repeat,
       CASE WHEN YTD_repeat.crm_member_id IS NULL THEN 0 ELSE orders END AS YTD_repeat_orders,
       CASE WHEN YTD_repeat.crm_member_id IS NULL THEN 0 ELSE member_sales END AS YTD_repeat_member_sales
 FROM edw.f_member_order_detail trans
 LEFT JOIN (
             SELECT crm_member_id,
                    COUNT(DISTINCT CASE WHEN if_eff_order_tag = 1 THEN original_order_id ELSE NULL END)                                             AS orders,
                    sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)    AS member_sales
            FROM edw.f_member_order_detail
            WHERE is_rrp_sales_type = 1 
              AND distributor_name <> 'LBR'
              AND date_id < current_date             -- end_date
              AND date_id >= '2024-01-01'            -- fixed
              AND crm_member_id IS NOT NULL
              AND distributor_name LIKE '%LCS%'      -- distributor_name
         GROUP BY 1
           HAVING COUNT(DISTINCT CASE WHEN if_eff_order_tag = 1 THEN original_order_id ELSE NULL END) >=2
            ) YTD_repeat
        ON trans.crm_member_id::integer = YTD_repeat.crm_member_id::integer
  WHERE is_rrp_sales_type = 1 
  AND  if_eff_order_tag = 1 
  AND distributor_name <> 'LBR'
  AND date_id < current_date              -- end_date
  AND date_id >= '2024-01-01'             -- fixed
  AND trans.crm_member_id IS NOT NULL
  AND distributor_name LIKE '%LCS%'   -- distributor_name
),

traffic_reg AS (
        SELECT '1 - new_member'::text                        AS member_type,
               CAST(SUM(traffic_amt) AS FLOAT)               AS traffic
          FROM dm.agg_final_sales_by_store_daily traffic_table
          WHERE agg_type = 'LEGO'
            AND CAST(traffic_table.date_id AS date) >= '2024-01-01'   -- start_date 
            AND CAST(traffic_table.date_id AS date) < current_date    -- end_date
            AND distributor LIKE '%LCS%'                              -- distributor_name
),

transaction_cte AS ( SELECT  
           trans.crm_member_id,
           if_eff_order_tag,
           distributor_name,
           trans.eff_reg_channel,
           CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' ELSE 'member' END AS is_member_sales, 
           trans.original_order_id,
           
           --------------------------------------------------------------------------
           CASE WHEN existing_retain_base_lcs.crm_member_id IS NOT NULL THEN 1 ELSE 0 END                  AS retained,
           CASE WHEN existing_reactivate_base_lcs.crm_member_id IS NOT NULL THEN 1 ELSE 0 END              AS reactivated,
           CASE WHEN existing_other_channel_reg_new_belong.crm_member_Id IS NOT NULL THEN 1 ELSE 0 END     AS other_channel_reg_new_belong, 
           
           CASE WHEN trans.crm_member_id IS NULL THEN '8 - non_member'
                WHEN new_member_lcs.member_detail_id IS NOT NULL THEN '1 - new_member'
                WHEN existing_never_purchased_base.member_detail_id IS NOT NULL THEN '3 - existing - never_purchased'
                WHEN existing_retain_base_lcs.crm_member_id IS NOT NULL THEN '4 - existing - retained_member'
                WHEN existing_reactivate_base_lcs.crm_member_id IS NOT NULL THEN '5 - existing - reactivated_member'
                WHEN existing_other_channel_reg_new_belong.crm_member_id IS NOT NULL THEN '7 - existing - other_channel_reg_new_belong'
                ELSE '6 - existing - 2024_member_shoppers'
           END                                                                                                                AS new_vs_existing_member_lcs,
           
                CASE WHEN trans.crm_member_id IS NULL THEN '10 - non_member'
                WHEN new_member_lcs.member_detail_id IS NOT NULL THEN '1 - new_member'
                WHEN existing_never_purchased_base.member_detail_id IS NOT NULL THEN '3 - existing - never_purchased'
                WHEN existing_3_month_purchased_base.crm_member_id IS NOT NULL THEN '9-1 - existing - 90天内购买'
                WHEN existing_3_month_not_purchased_base.crm_member_id IS NOT NULL THEN '9-2 - existing - 90天未购'
                WHEN existing_6_month_not_purchased_base.crm_member_id IS NOT NULL THEN '9-3 - existing - 180天未购'
                WHEN existing_9_month_not_purchased_base.crm_member_id IS NOT NULL THEN '9-4 - existing - 270天未购'
                WHEN existing_12_month_and_longer_not_purchased_base.crm_member_id IS NOT NULL THEN '9-5 - existing - 360天以上未购'
                WHEN existing_other_channel_reg_new_belong.crm_member_id IS NOT NULL THEN '7 - existing - other_channel_reg_new_belong'
           END                                                                                                                AS new_vs_existing_member_lcs_v2,
           
           sales_qty,
           order_rrp_amt

   FROM edw.f_member_order_detail trans
   LEFT JOIN (SELECT DISTINCT member_detail_id FROM new_member_base) new_member_lcs
           ON trans.crm_member_id::integer = new_member_lcs.member_detail_id::integer
  LEFT JOIN new_member_base new_member_distributor
       ON trans.crm_member_id::integer = new_member_distributor.member_detail_id::integer
      AND trans.distributor_name = new_member_distributor.eff_reg_channel
  LEFT JOIN existing_never_purchased_base
        ON trans.crm_member_id::integer = existing_never_purchased_base.member_detail_id::integer 
  LEFT JOIN (SELECT DISTINCT crm_member_id FROM existing_retain_base) existing_retain_base_lcs
         ON trans.crm_member_id::integer = existing_retain_base_lcs.crm_member_id::integer
  LEFT JOIN (SELECT DISTINCT crm_member_id FROM existing_reactivate_base) existing_reactivate_base_lcs
         ON trans.crm_member_id::integer = existing_reactivate_base_lcs.crm_member_id::integer
  LEFT JOIN existing_other_channel_reg_new_belong
         ON trans.crm_member_id::integer = existing_other_channel_reg_new_belong.crm_member_id::integer
 -----------------------------------------------------------------
 
  LEFT JOIN existing_3_month_purchased_base
         ON trans.crm_member_id::integer = existing_3_month_purchased_base.crm_member_id::integer
  LEFT JOIN existing_3_month_not_purchased_base
         ON trans.crm_member_id::integer = existing_3_month_not_purchased_base.crm_member_id::integer
  LEFT JOIN existing_6_month_not_purchased_base
         ON trans.crm_member_id::integer = existing_6_month_not_purchased_base.crm_member_id::integer
  LEFT JOIN existing_9_month_not_purchased_base
         ON trans.crm_member_id::integer = existing_9_month_not_purchased_base.crm_member_id::integer
  LEFT JOIN existing_12_month_and_longer_not_purchased_base
         ON trans.crm_member_id::integer = existing_12_month_and_longer_not_purchased_base.crm_member_id::integer
       WHERE is_rrp_sales_type = 1 
          AND distributor_name <> 'LBR'
          AND date_id >= '2024-01-01'             -- start_date 
          AND date_id < current_date              -- end_date
          AND distributor_name LIKE '%LCS%'       -- distributor_name

),



transaction_summary AS (
SELECT new_vs_existing_member_lcs AS member_type,
      sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)                                                               AS ttl_sales,
      sum(case when is_member_sales = 'member' AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_sales = 'member' AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS member_sales,
      COUNT(DISTINCT CASE WHEN if_eff_order_tag = 1 THEN crm_member_id ELSE NULL END)                                                                                                            AS member_shopper,
      COUNT(DISTINCT CASE WHEN if_eff_order_tag = 1 AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END)                                                                          AS member_orders
   FROM transaction_cte
GROUP BY 1
),

transaction_summary_v2 AS (
SELECT new_vs_existing_member_lcs_v2 AS member_type,
      sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)                                                               AS ttl_sales,
      sum(case when is_member_sales = 'member' AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_sales = 'member' AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS member_sales,
      COUNT(DISTINCT CASE WHEN if_eff_order_tag = 1 THEN crm_member_id ELSE NULL END)                                                                                                            AS member_shopper,
      COUNT(DISTINCT CASE WHEN if_eff_order_tag = 1 AND crm_member_id IS NOT NULL THEN original_order_id ELSE NULL END)                                                                          AS member_orders
   FROM transaction_cte
GROUP BY 1
),


member_base_summary AS (
SELECT DISTINCT '8 - non_member' AS member_type, 0 AS member_base FROM new_member_base
UNION ALL
SELECT '1 - new_member' AS member_type, COUNT(DISTINCT member_detail_id) AS member_base FROM new_member_base
UNION ALL
SELECT '3 - existing - never_purchased' AS member_type, COUNT(DISTINCT member_detail_id) AS member_base FROM existing_never_purchased_base 
UNION ALL
SELECT '4 - existing - retained_member' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_retain_base
UNION ALL
SELECT '5 - existing - reactivated_member' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_reactivate_base
UNION ALL
SELECT '6 - existing - 2024_member_shoppers' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_2024_initial_purchase_base
UNION ALL
SELECT '7 - existing - other_channel_reg_new_belong ' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_other_channel_reg_new_belong
),


all_summary AS (
SELECT member_base_summary.member_type,
      member_base_summary.member_base,
      transaction_summary.ttl_sales,
      transaction_summary.member_sales,
      transaction_summary.member_shopper,
      transaction_summary.member_orders
  FROM member_base_summary
  LEFT JOIN transaction_summary
         ON member_base_summary.member_type = transaction_summary.member_type
UNION ALL
SELECT '2 - existing'                             AS member_type,
      SUM(member_base_summary.member_base)    AS member_base,
      SUM(transaction_summary.ttl_sales)      AS ttl_sales,
      SUM(transaction_summary.member_sales)   AS member_sales,
      SUM(transaction_summary.member_shopper) AS member_shopper,
      SUM(transaction_summary.member_orders)  AS member_orders
  FROM member_base_summary
  LEFT JOIN transaction_summary
         ON member_base_summary.member_type = transaction_summary.member_type
  WHERE member_base_summary.member_type LIKE '%existing%'
UNION ALL 
SELECT '0 - ttl'                                  AS member_type,
      SUM(member_base_summary.member_base)    AS member_base,
      SUM(transaction_summary.ttl_sales)      AS ttl_sales,
      SUM(transaction_summary.member_sales)   AS member_sales,
      SUM(transaction_summary.member_shopper) AS member_shopper,
      SUM(transaction_summary.member_orders)  AS member_orders
  FROM member_base_summary
  LEFT JOIN transaction_summary
         ON member_base_summary.member_type = transaction_summary.member_type
UNION ALL
SELECT 'YTD repeat purchase'::text AS member_type,
        COUNT(DISTINCT crm_member_id) AS member_base,
        SUM(YTD_repeat_member_sales)  AS ttl_sales,
        SUM(YTD_repeat_member_sales)  AS member_sales,
        COUNT(DISTINCT CASE WHEN ytd_repeat = 1 THEN crm_member_id ELSE NULL END) AS member_shopper,
        SUM(YTD_repeat_orders)        AS member_orders
  FROM YTD_member_shopper_and_repeat_shopper
),

member_base_summary_v2 AS (
SELECT '9-1 - existing - 90天内购买 ' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_3_month_purchased_base
UNION ALL
SELECT '9-2 - existing - 90天未购' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_3_month_not_purchased_base
UNION ALL
SELECT '9-3 - existing - 180天未购' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_6_month_not_purchased_base
UNION ALL
SELECT '9-4 - existing - 270天未购' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_9_month_not_purchased_base
UNION ALL
SELECT '9-5 - existing - 360天以上未购' AS member_type, COUNT(DISTINCT crm_member_id) AS member_base FROM existing_12_month_and_longer_not_purchased_base
),

all_summary_v2 AS (
SELECT member_base_summary_v2.member_type,
      member_base_summary_v2.member_base,
      transaction_summary_v2.ttl_sales,
      transaction_summary_v2.member_sales,
      transaction_summary_v2.member_shopper,
      transaction_summary_v2.member_orders
  FROM member_base_summary_v2
  LEFT JOIN transaction_summary_v2
         ON member_base_summary_v2.member_type = transaction_summary_v2.member_type
),

final_1 AS (
SELECT all_summary.member_type                                                          AS member_type,
      all_summary.ttl_sales                                                             AS ttl_sales,
       
      traffic_reg.traffic                                                                  AS traffic,
      ISNULL(ROUND(CAST(member_base AS FLOAT)/NULLIF(traffic_reg.traffic,0),4),NULL)       AS registration_rate,
       
      all_summary.member_base                                                          AS member_base,
      ISNULL(CAST(member_shopper AS FLOAT)/NULLIF(member_base,0),0)                       AS CR,
      all_summary.member_shopper                                                       AS member_shopper,
      ISNULL(CAST(member_orders AS FLOAT)/NULLIF(member_shopper,0),0)                     AS member_frequency,
      ISNULL(CAST(member_sales AS FLOAT)/NULLIF(member_orders,0),0)                       AS member_atv,
      all_summary.member_sales                                                         AS member_sales
FROM all_summary
LEFT JOIN traffic_reg
     ON all_summary.member_type::text = traffic_reg.member_type::text
),

final_2 AS (
SELECT all_summary_v2.member_type                                                          AS member_type,
      all_summary_v2.ttl_sales                                                             AS ttl_sales,
       
      traffic_reg.traffic                                                                  AS traffic,
      ISNULL(ROUND(CAST(member_base AS FLOAT)/NULLIF(traffic_reg.traffic,0),4),NULL)       AS registration_rate,
       
      all_summary_v2.member_base                                                          AS member_base,
      ISNULL(CAST(member_shopper AS FLOAT)/NULLIF(member_base,0),0)                       AS CR,
      all_summary_v2.member_shopper                                                       AS member_shopper,
      ISNULL(CAST(member_orders AS FLOAT)/NULLIF(member_shopper,0),0)                     AS member_frequency,
      ISNULL(CAST(member_sales AS FLOAT)/NULLIF(member_orders,0),0)                       AS member_atv,
      all_summary_v2.member_sales                                                         AS member_sales
FROM all_summary_v2
LEFT JOIN traffic_reg
     ON all_summary_v2.member_type::text = traffic_reg.member_type::text
),

final_1_and_2 AS (
SELECT *
 FROM final_1
UNION ALL
SELECT *
  FROM final_2
  )
  
  SELECT final_1_and_2.*,
         still_need_to_recall_table.still_need_to_recall,
         to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date,
         getdate()                                                   AS dl_load_time
     FROM final_1_and_2
LEFT JOIN (
                SELECT '4 - existing - retained_member'::text AS member_type, COUNT(DISTINCT crm_member_id) AS still_need_to_recall FROM have_not_retained
                 UNION ALL
                 SELECT '5 - existing - reactivated_member'::text AS member_type, COUNT(DISTINCT crm_member_id) AS still_need_to_recall FROM have_not_reactivated
                 UNION ALL
                SELECT '9-2 - existing - 90天未购'::text AS member_type, COUNT(DISTINCT crm_member_id) AS still_need_to_recall FROM have_not_purchased_3_month
                 UNION ALL
                 SELECT '9-3 - existing - 180天未购'::text AS member_type, COUNT(DISTINCT crm_member_id) AS still_need_to_recall FROM have_not_purchased_6_month
                 UNION ALL
                 SELECT '9-4 - existing - 270天未购'::text AS member_type, COUNT(DISTINCT crm_member_id) AS still_need_to_recall FROM have_not_purchased_9_month
                  UNION ALL
                 SELECT '9-5 - existing - 360天以上未购'::text AS member_type, COUNT(DISTINCT crm_member_id) AS still_need_to_recall FROM have_not_purchased_12_month_and_longer
                 UNION ALL
                  SELECT 'YTD repeat purchase'::text AS member_type, COUNT(DISTINCT CASE WHEN YTD_repeat = 0 THEN crm_member_Id ELSE NULL END) AS still_need_to_recall FROM YTD_member_shopper_and_repeat_shopper
            ) still_need_to_recall_table
        ON final_1_and_2.member_type = still_need_to_recall_table.member_type