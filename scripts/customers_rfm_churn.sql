CREATE OR REPLACE VIEW gold.RFM_Churn_view AS
WITH RawData AS (
    /*
    Select columns needed for analyses
    */
    SELECT 
        CustomerID,
        DATEDIFF(day, MAX(LastEditedDate), GETDATE()) AS Recency, -- Time since last purchase
        COUNT(OrderID) AS Frequency, -- Total orders
        round(SUM(unitprice * Quantity),2) AS Monetary, -- Total spend,
        DATEDIFF(day, MAX(LastEditedWhen), GETDATE()) AS days_since_last_purchase,
        -- Calculate the average time between purchases (Inter-purchase Time)
        DATEDIFF(day, MIN(LastEditedWhen), MAX(LastEditedWhen)) / NULLIF(COUNT(OrderID) - 1, 0) AS avg_purchase_gap
    FROM gold.orders_fact
    GROUP BY CustomerID
),
RankedData AS (
    /*
    Assign Scoring (Quintiles) 
      •	Recency: Lower values (more recent) get higher scores (rank by Recency DESC).
      •	Frequency/Monetary: Higher values get higher scores (rank by ASC).
    */
    SELECT 
        CustomerID,
        NTILE(5) OVER (ORDER BY Recency DESC) AS R_Score,
        NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
        NTILE(5) OVER (ORDER BY Monetary ASC) AS M_Score
    from 
      RawData 

),
RFM_DATA as (
  SELECT 
      /*
        Segment Your Customers
      */
      CustomerID,
      R_Score, F_Score, M_Score,
      CASE 
          WHEN R_Score > 4 AND F_Score > 4  THEN 'VIP Customers'
          WHEN R_Score <= 2 AND F_Score >= 4 THEN 'AT risk Giant Customers'
          WHEN R_Score >= 4 AND F_Score <= 2 THEN 'Promising New Customers'
          ELSE 'Regular Customers'
      END AS Customer_Segment
  FROM RankedData
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
    FROM RawData
),
Risk_Data AS (
    SELECT 
        CustomerID,
        deviation_ratio,
        CASE 
            WHEN deviation_ratio <= 1.0 THEN 'Active'
            WHEN deviation_ratio BETWEEN 1.1 AND 2.5 THEN 'Slipping'
            ELSE 'Lapsed'
        END AS health_status
    FROM Risk_Scoring -- Calculated in previous steps
)
SELECT 
    r.customer_segment,
    k.health_status,
    COUNT(r.CustomerID) AS customer_count,
    ROUND(AVG(r.M_Score), 2) AS avg_monetary_tier,
    -- Actionable Metric: Total Revenue at Stake
    SUM(CASE WHEN k.health_status = 'Slipping' THEN 1 ELSE 0 END) AS intervention_needed_count
FROM RFM_Data r
JOIN Risk_Data k ON r.CustomerID = k.CustomerID
GROUP BY 1, 2
ORDER BY 1, 2;