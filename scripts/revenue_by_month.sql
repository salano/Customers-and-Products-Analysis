create or replace view gold.revenue_by_month AS
-- Analyse revenue performance over time
-- Quick DATE_TRUNC() Functions

SELECT
    DATE_TRUNC('month', LastEditedWhen) AS order_date,
    SUM(of.Quantity * of.UnitPrice) AS total_revenue,
    COUNT(DISTINCT CustomerID) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.orders_fact of
WHERE LastEditedWhen IS NOT NULL
GROUP BY DATE_TRUNC('month', LastEditedWhen)
ORDER BY DATE_TRUNC('month', LastEditedWhen);
