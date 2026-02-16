create or replace view gold.customer_segment_part_to_Whole as
-- Which customer segments contribute the most to overall revenue?
WITH segment_revenue AS (
    SELECT
        customer_segment,
        round(SUM(total_revenue),2) AS total_revenue,
        COUNT(total_products) AS total_products,
        round(SUM(avg_monthly_spend),2) AS total_avg_monthly_spend
    FROM gold.customer_segment
    GROUP BY customer_segment
)
SELECT
    customer_segment,
    total_products,
    total_revenue,
    total_avg_monthly_spend,
    ROUND((CAST(total_revenue AS FLOAT) / SUM(total_revenue) OVER ()) * 100, 2) AS percentage_of_total,
    RANK() OVER (ORDER BY total_revenue DESC) AS rank_customer
FROM segment_revenue;