create or replace view gold.brands_part_to_whole as
-- Which brands contribute the most to overall revenue?
WITH brand_revenue AS (
    SELECT
        p.Brand,
        round(SUM(of.Quantity * of.UnitPrice),2) AS total_revenue
    FROM gold.orders_fact of
    LEFT JOIN gold.products p
        ON p.ProductID = of.ProductID
    GROUP BY p.Brand
)

SELECT
    Brand,
    total_revenue,
    SUM(total_revenue) OVER () AS overall_revenue,
    ROUND((CAST(total_revenue AS FLOAT) / SUM(total_revenue) OVER ()) * 100, 2) AS percentage_of_total
FROM brand_revenue
ORDER BY total_revenue DESC;
