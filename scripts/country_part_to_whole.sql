create or replace view gold.country_part_to_whole as
-- Which Country/Region contribute the most to overall revenue?
WITH country_revenue AS (
    SELECT
        c.Country,
        round(SUM(of.Quantity * of.UnitPrice),2) AS total_revenue
    FROM gold.orders_fact of
    LEFT JOIN gold.customers c
        ON c.CustomerID = of.CustomerID
    GROUP BY c.Country
)

SELECT
    Country,
    total_revenue,
    SUM(total_revenue) OVER () AS overall_revenue,
    ROUND((CAST(total_revenue AS FLOAT) / SUM(total_revenue) OVER ()) * 100, 2) AS percentage_of_total
FROM country_revenue
ORDER BY total_revenue DESC;