create view gold.RFM_view as 
WITH RawData AS (
    /*
    Select columns needed for analyses
    */
    SELECT 
        CustomerID,
        DATEDIFF(day, MAX(LastEditedDate), GETDATE()) AS Recency, -- Time since last purchase
        COUNT(OrderID) AS Frequency, -- Total orders
        round(SUM(unitprice * Quantity),2) AS Monetary -- Total spend
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

)
SELECT 
    /*
      Segment Your Customers
    */
    CustomerID,
    CONCAT(R_Score, F_Score, M_Score) AS RFM_Cell, -- Combined 3-digit score
    CASE 
        WHEN R_Score = 5 AND F_Score = 5 AND M_Score = 5 THEN 'VIP Customers'
        WHEN R_Score >= 4 AND F_Score >= 4 THEN 'Loyal Customers'
        WHEN R_Score = 5 AND F_Score = 1 THEN 'New Customers'
        WHEN R_Score <= 2 THEN 'At Risk'
        ELSE 'Other'
    END AS Customer_Segment
FROM RankedData;