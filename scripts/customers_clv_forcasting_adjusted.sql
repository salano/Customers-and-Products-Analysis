create or replace view gold.predicted_customer_lifetime_value_adjusted as 
WITH Customer_Behavior AS (
  /*
    Identify Risk Indicators
    We calculate indicators that signal a fading relationship, specifically Recency vs. Average Gap. If a customer usually buys every 30 days but hasn't bought in 60, their risk is rising

  */
    SELECT 
      /*
      Calculate key indicators
      */
        CustomerID,
        DATEDIFF(day, MAX(LastEditedWhen), GETDATE()) AS days_since_last_purchase,
        COUNT(OrderID) AS total_orders,
        -- Calculate the average time between purchases (Inter-purchase Time)
        DATEDIFF(day, MIN(LastEditedWhen), MAX(LastEditedWhen)) / NULLIF(COUNT(OrderID) - 1, 0) AS avg_purchase_gap
    FROM gold.orders_fact
    GROUP BY CustomerID
),
Risk_Scoring AS (
  /*
    Calculate the Churn Risk Score
    We use a ratio of Current Silence / Typical Gap.
    -	Score 0-1.0: Behavior is normal.
    -	Score > 1.5: Customer is "overdue" and likely drifting.
    -	Score > 3.0: High probability of churn.

  */
    SELECT 
        *,
        CASE 
            WHEN avg_purchase_gap IS NULL THEN 0.5 -- New customers (neutral risk)
            ELSE (CAST(days_since_last_purchase AS FLOAT) / NULLIF(avg_purchase_gap, 0))
        END AS deviation_ratio
    FROM Customer_Behavior
)
SELECT 
/*
  Refine the CLV Forecast
  - Integrate the risk score into your prediction
*/
    CustomerID,
    deviation_ratio,
    CASE 
        WHEN deviation_ratio < 1.0 THEN 'Low'
        WHEN deviation_ratio BETWEEN 1.0 AND 2.0 THEN 'Medium'
        ELSE 'High'
    END AS risk_category,
    -- Survival Probability: The higher the deviation, the lower the probability
    CASE 
        WHEN deviation_ratio <= 1.0 THEN 1.0  -- 100% likely to stay
        WHEN deviation_ratio > 3.0 THEN 0.1   -- 10% likely to stay
        ELSE (1 / deviation_ratio)            -- Sliding scale
    END AS survival_prob,
    -- Refined CLV: (Historical Annual Value) * Survival Probability
    round((total_orders * survival_prob)) AS adjusted_expected_orders_yr
FROM Risk_Scoring;
/*
Note
Why this Refines Forecasts
-	Avoids Over-Optimism: Without this, a "VIP Customer" who hasn't bought in 2 years would still show a massive predicted CLV.
-	Triggers Intervention: You can export customers where risk_category = 'Medium' directly to a Marketing Automation tool  for a "Win-back" campaign.
-	Dynamic Budgeting: It allows finance teams to see a "Risk-Adjusted" revenue pipeline.


*/