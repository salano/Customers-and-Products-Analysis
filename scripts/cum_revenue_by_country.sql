create or replace view gold.cum_rev_by_country AS 
-- Calculate the total sales per month by Country/Region
-- and the running total of revenue over time by Country/Region

SELECT
  country,
	order_date,
	total_revenue,
	SUM(total_revenue) OVER (ORDER BY order_date) AS running_total_revenue,
	AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
    SELECT
        c.Country, 
        DATE_TRUNC('year', of.LastEditedWhen) AS order_date,
        round(SUM(of.Quantity * of.UnitPrice),2) AS total_revenue,
        round(AVG(of.UnitPrice),2) AS avg_price
    FROM gold.orders_fact of
    join gold.customers c
        ON of.CustomerID = c.CustomerID
    WHERE of.LastEditedWhen IS NOT NULL and c.active_flg = 1
    GROUP BY c.Country, DATE_TRUNC('year', of.LastEditedWhen)
) t
order by Country, order_date
