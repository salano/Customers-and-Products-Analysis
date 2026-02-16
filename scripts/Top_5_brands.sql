create or replace view gold.top_5_brands AS 
SELECT *
/*
Ranking analysis using a flexible window function approach
Alternative, use TO and GROUP BY
*/
FROM (
    SELECT
        p.Brand,
        round(SUM(of.Quantity * of.UnitPrice),2) AS total_revenue,
        RANK() OVER (ORDER BY SUM(of.Quantity * of.UnitPrice) DESC) AS rank_brands
    FROM gold.orders_fact of
    LEFT JOIN gold.products p
        ON p.ProductID = of.ProductID
    GROUP BY p.Brand
) AS ranked_brands
WHERE rank_brands <= 5;
