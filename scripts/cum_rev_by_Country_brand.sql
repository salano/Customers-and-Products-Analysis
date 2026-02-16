create or replace view gold.cum_rev_by_Country_brand AS
-- Calculate the total revenue per year by brand
-- and the running total of revenue over time by brand

SELECT
  Country,
  Brand,
	order_date,
	total_revenue,
	SUM(total_revenue) OVER (ORDER BY order_date) AS running_total_revenue,
	AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
    SELECT
        c.Country,
        p.Brand, 
        DATE_TRUNC('year', of.LastEditedWhen) AS order_date,
        round(SUM(of.Quantity * of.UnitPrice),2) AS total_revenue,
        round(AVG(of.UnitPrice),2) AS avg_price
    FROM gold.orders_fact of
    left join gold.products p
        ON of.ProductID = p.ProductID
    left join gold.customers c
        ON of.CustomerID = c.CustomerID
    WHERE of.LastEditedWhen IS NOT NULL and p.active_flg = 1
    GROUP BY c.Country, P.Brand, DATE_TRUNC('year', of.LastEditedWhen)
) t
where order_date < to_date('2025-01-01')
order by brand, order_date
