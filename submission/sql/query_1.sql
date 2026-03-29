WITH base AS (
  SELECT
    to_date(date_format(order_date,'yyyy-MM-01')) AS month,
    product_category_name,
    sum(price+freight_value) AS monthly_revenue
  FROM fact_order_items f 
  join dim_products d
  on f.product_key = d.product_key
  GROUP BY to_date(date_format(order_date,'yyyy-MM-01')), product_category_name having count(order_date) > 10
),

monthly_revenue_rank as( SELECT *,
  dense_rank() OVER (
    PARTITION BY month
    ORDER BY monthly_revenue DESC
  ) AS rnk
FROM base
),
overall_revenue as (
select product_category_name, sum(monthly_revenue) as overall_revenue from base group by product_category_name
)

select m.product_category_name,month,monthly_revenue, date_format(month, 'yyyy') AS YEAR,
      lag(monthly_revenue) over (partition by m.product_category_name order by month) as prev_month_revenue,
      ROUND( (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month))
    / LAG(monthly_revenue) OVER (ORDER BY month) * 100,
    2
  ) AS mom_growth_pct,
  avg(monthly_revenue) over (partition by m.product_category_name order by month range Between 2 preceding and current row  )as rolling_3m_avg_revenue

from monthly_revenue_rank m join overall_revenue o 
on m.product_category_name = o.product_category_name