create or replace view gold.cum_rev AS
-- Calculate the total revenue per year 
-- and the running total of revenue over time 

SELECT
	order_date,
	total_revenue,
	SUM(total_revenue) OVER (ORDER BY order_date) AS running_total_revenue,
	AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
    SELECT 
        DATE_TRUNC('year', of.LastEditedWhen) AS order_date,
        round(SUM(of.Quantity * of.UnitPrice),2) AS total_revenue,
        round(AVG(of.UnitPrice),2) AS avg_price
    FROM gold.orders_fact of
    WHERE of.LastEditedWhen IS NOT NULL
    GROUP BY DATE_TRUNC('year', of.LastEditedWhen)
) t
