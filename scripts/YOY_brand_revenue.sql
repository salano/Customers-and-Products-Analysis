Create or replace view gold.YOY_Brands_Revenue AS
/* Analyze the yearly performance of brands by comparing their revenue 
to both the average revenue performance of the brands and the previous year's revenue */
WITH yearly_brands_revenue AS (
    SELECT
        YEAR(of.LastEditedWhen) AS order_year,
        p.Brand,
        round(SUM(of.Quantity * of.UnitPrice),2) AS current_revenue
    FROM gold.orders_fact of
    LEFT JOIN gold.products p
        ON of.ProductID = p.ProductID
    WHERE of.LastEditedWhen IS NOT NULL
    GROUP BY 
        YEAR(of.LastEditedWhen),
        p.Brand
)
SELECT
    order_year,
    Brand,
    current_revenue,
    round(AVG(current_revenue) OVER (PARTITION BY Brand),2) AS avg_revenue,
    current_revenue - round(AVG(current_revenue) OVER (PARTITION BY Brand),2) AS diff_avg,
    CASE 
        WHEN current_revenue - round(AVG(current_revenue) OVER (PARTITION BY Brand),2) > 0 THEN 'Above Avg'
        WHEN current_revenue - round(AVG(current_revenue) OVER (PARTITION BY Brand),2) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
    -- Year-over-Year Analysis
    LAG(current_revenue) OVER (PARTITION BY Brand ORDER BY order_year) AS py_revenue,
    current_revenue - LAG(current_revenue) OVER (PARTITION BY Brand ORDER BY order_year) AS diff_py,
    CASE 
        WHEN current_revenue - LAG(current_revenue) OVER (PARTITION BY Brand ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_revenue - LAG(current_revenue) OVER (PARTITION BY Brand ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change
FROM yearly_brands_revenue
ORDER BY Brand, order_year;
