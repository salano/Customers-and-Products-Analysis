create or replace view gold.predicted_customer_lifetime_value as 
WITH Customer_Base AS (
    SELECT 
      /*
      Select main columns for analyses
      */
        CustomerID,
        MIN(LastEditedWhen) as first_purchase,
        MAX(LastEditedWhen) as last_purchase,
        COUNT(DISTINCT OrderID) as total_orders,
        round(SUM(UnitPrice * Quantity),2) as total_revenue,
        round(AVG(UnitPrice * Quantity),2) as avg_order_value -- Average Purchase Value (APV)
    FROM gold.orders_fact
    GROUP BY CustomerID
),
Ratios AS (
  /*
    Calculate Key Prediction Ratios
    -	APFR: How many times they buy in a given time unit (e.g., per year).
    -	Lifespan: The duration in years between their first and most recent purchase

    NB To forecast, you need the Average Purchase Frequency Rate (APFR) and the Average Customer Lifespan (ACL). 

  */
    SELECT 
        CustomerID,
        avg_order_value,
        -- Frequency: orders per year
        (CAST(total_orders AS FLOAT) / 
         NULLIF(DATEDIFF(day, first_purchase, last_purchase), 0) * 365) AS annual_frequency,
        -- Lifespan: total years active
        DATEDIFF(day, first_purchase, last_purchase) / 365.0 AS current_lifespan_years
    FROM Customer_Base
)SELECT 
  /*
    Forecast Future CLV
  */
    CustomerID,
    -- Simple Forecast: (Avg Order Value * Annual Frequency) * Predicted Future Years
    (avg_order_value * annual_frequency) * 3 AS predicted_3yr_clv,
    
    -- Discounted Forecast: accounting for a 10% annual churn probability
    (avg_order_value * annual_frequency) * (1 - POWER(0.9, 3)) / (1 - 0.9) AS discounted_3yr_clv
FROM Ratios;